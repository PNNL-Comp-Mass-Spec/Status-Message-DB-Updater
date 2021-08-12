using System;
using System.Collections.Generic;
using Apache.NMS;
using Apache.NMS.ActiveMQ;
using Apache.NMS.ActiveMQ.Commands;
using PRISM;

namespace StatusMessageDBUpdater
{
    // received input messages are sent to a delegate function with this signature
    public delegate void MessageReceivedDelegate(string processor, string message);

    // received commands are sent to a delegate function with this signature
    public delegate void MessageProcessorDelegate(string cmdText);

    internal class MessageHandler : EventNotifier, IDisposable
    {
        private IConnection mConnection;
        private ISession mStatusSession;
        private IMessageProducer mStatusSender;
        private IMessageConsumer mInputConsumer;
        private IMessageConsumer mBroadcastConsumer;

        private bool mIsDisposed;
        private bool mHasConnection;
        private bool mConnectionHasException = false;

        public event MessageReceivedDelegate InputMessageReceived;
        public event MessageProcessorDelegate BroadcastReceived;

        public string BrokerUri { get; set; }

        public string InputStatusTopicName { get; set; }

        public string BroadcastTopicName { get; set; }

        public string OutputStatusTopicName { get; set; }

        /// <summary>
        /// Create set of NMS connection objects necessary to talk to the ActiveMQ broker
        /// </summary>
        /// <param name="retryCount">Number of times to try the connection</param>
        /// <param name="timeoutSeconds">Number of seconds to wait for the broker to respond</param>
        protected void CreateConnection(int retryCount = 2, int timeoutSeconds = 15)
        {
            if (mHasConnection)
                return;

            if (retryCount < 0)
                retryCount = 0;

            var retriesRemaining = retryCount;

            if (timeoutSeconds < 5)
                timeoutSeconds = 5;

            var errorList = new List<string>();

            while (retriesRemaining >= 0)
            {
                try
                {
                    // Broker URI should be in the form
                    // tcp://Proto-7.pnl.gov:61616
                    //  or
                    // failover:(tcp://Proto-7.pnl.gov:61616,tcp://proto-4.pnl.gov:61616)

                    IConnectionFactory connectionFactory = new ConnectionFactory(BrokerUri);
                    mConnection = connectionFactory.CreateConnection();
                    mConnection.RequestTimeout = new TimeSpan(0, 0, timeoutSeconds);
                    mConnection.Start();

                    mHasConnection = true;
                    var username = System.Security.Principal.WindowsIdentity.GetCurrent().Name;

                    OnStatusEvent(string.Format("Connected to broker as user {0}: {1}", username, BrokerUri));

                    mConnectionHasException = false;
                    mConnection.ExceptionListener += ConnectionExceptionListener;

                    return;
                }
                catch (Exception ex)
                {
                    // Connection failed
                    if (!errorList.Contains(ex.Message))
                        errorList.Add(ex.Message);

                    // Sleep for 3 seconds
                    System.Threading.Thread.Sleep(3000);
                }

                retriesRemaining -= 1;
            }

            // If we get here, we never could connect to the message broker

            var msg = "Exception creating broker connection";
            if (retryCount > 0)
                msg += " after " + (retryCount + 1) + " attempts";

            msg += ": " + string.Join("; ", errorList);

            OnErrorEvent(msg);
        }

        private void ConnectionExceptionListener(Exception exception)
        {
            // NOTE: If the connection was resumable, we could use ConnectionInterruptedListener, but a straight tcp connection is not resumable
            OnStatusEvent("ActiveMQ connection exception received: " + exception.Message);
            mConnectionHasException = true;
        }

        /// <summary>
        /// If the connection has fired an exception, it's probably now broken, and therefore invalid until we re-connect.
        /// </summary>
        /// <returns></returns>
        public bool IsConnectionBroken()
        {
            return mConnectionHasException;
        }

        /// <summary>
        /// Create the message broker communication objects and register the listener function
        /// </summary>
        /// <returns>TRUE for success; FALSE otherwise</returns>
        public bool Init()
        {
            try
            {
                if (!mHasConnection)
                    CreateConnection();

                if (!mHasConnection)
                    return false;

                var inputSession = mConnection.CreateSession();
                mInputConsumer = inputSession.CreateConsumer(new ActiveMQTopic(InputStatusTopicName));
                mInputConsumer.Listener += OnInputMessageReceived;
                OnStatusEvent("Input listener established for topic '" + InputStatusTopicName + "'");

                // topic for commands broadcast to all folder makers
                var broadcastSession = mConnection.CreateSession();
                mBroadcastConsumer = broadcastSession.CreateConsumer(new ActiveMQTopic(BroadcastTopicName));
                mBroadcastConsumer.Listener += OnBroadcastReceived;
                OnStatusEvent("Broadcast listener established for topic '" + BroadcastTopicName + "'");

                // topic to send status information over
                mStatusSession = mConnection.CreateSession();
                mStatusSender = mStatusSession.CreateProducer(new ActiveMQTopic(OutputStatusTopicName));
                OnStatusEvent("Status sender established for topic '" + OutputStatusTopicName + "'");

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception while initializing message sessions", ex);
                DestroyConnection();
                return false;
            }
        }

        /// <summary>
        /// Command listener function. Received commands will cause this to be called
        ///	and it will trigger an event to pass on the command to all registered listeners
        /// </summary>
        /// <param name="message">Incoming message</param>
        private void OnInputMessageReceived(IMessage message)
        {
            if (!(message is ITextMessage textMessage))
                return;

            var processor = message.Properties.GetString("ProcessorName");
            InputMessageReceived?.Invoke(processor, textMessage.Text);
        }

        /// <summary>
        /// Broadcast listener function. Received Broadcasts will cause this to be called
        ///	and it will trigger an event to pass on the command to all registered listeners
        /// </summary>
        /// <param name="message">Incoming message</param>
        private void OnBroadcastReceived(IMessage message)
        {
            if (!(message is ITextMessage textMessage))
                return;

            OnDebugEvent("MessageHandler(), Broadcast message received");
            if (BroadcastReceived != null)
            {
                // call the delegate to process the command
                BroadcastReceived(textMessage.Text);
            }
            else
            {
                OnDebugEvent("MessageHandler().OnBroadcastReceived: No event handlers assigned");
            }
        }

        /// <summary>
        /// Sends a status message
        /// </summary>
        /// <param name="processor"></param>
        /// <param name="message">Outgoing message string</param>
        public void SendMessage(string processor, string message)
        {
            if (!mIsDisposed)
            {
                var textMessage = mStatusSession.CreateTextMessage(message);
                textMessage.NMSTimeToLive = TimeSpan.FromMinutes(60);
                textMessage.NMSDeliveryMode = MsgDeliveryMode.NonPersistent;
                textMessage.Properties.SetString("ProcessorName", processor);
                try
                {
                    mStatusSender.Send(textMessage);
                }
                catch
                {
                    // Do nothing
                }
            }
            else
            {
                throw new ObjectDisposedException(GetType().FullName);
            }
        }

        /// <summary>
        /// Cleans up a connection after error or when closing
        /// </summary>
        protected void DestroyConnection()
        {
            if (mHasConnection)
            {
                mConnection.Dispose();
                mHasConnection = false;
            }
        }

        /// <summary>
        /// Implements IDisposable interface
        /// </summary>
        public void Dispose()
        {
            if (mIsDisposed)
                return;

            DestroyConnection();
            mIsDisposed = true;
        }
    }
}
