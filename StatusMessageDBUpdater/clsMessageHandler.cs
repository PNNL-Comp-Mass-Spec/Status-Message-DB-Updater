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

    class clsMessageHandler : clsEventNotifier, IDisposable
    {
        #region "Class variables"

        private string m_BrokerUri;
        private string m_InputStatusTopicName;
        private string m_BroadcastTopicName;
        private string m_OutputStatusTopicName;

        private IConnection m_Connection;
        private ISession m_StatusSession;
        private IMessageProducer m_StatusSender;
        private IMessageConsumer m_InputConsumer;
        private IMessageConsumer m_BroadcastConsumer;

        private bool m_IsDisposed;
        private bool m_HasConnection;

        #endregion

        #region "Events"

        public event MessageReceivedDelegate InputMessageReceived;
        public event MessageProcessorDelegate BroadcastReceived;

        #endregion

        #region "Properties"

        public string BrokerUri
        {
            get => m_BrokerUri;
            set => m_BrokerUri = value;
        }

        public string InputStatusTopicName
        {
            get => m_InputStatusTopicName;
            set => m_InputStatusTopicName = value;
        }

        public string BroadcastTopicName
        {
            get => m_BroadcastTopicName;
            set => m_BroadcastTopicName = value;
        }

        public string OutputStatusTopicName
        {
            get => m_OutputStatusTopicName;
            set => m_OutputStatusTopicName = value;
        }
        #endregion

        #region "Methods"

        /// <summary>
        /// Create set of NMS connection objects necessary to talk to the ActiveMQ broker
        /// </summary>
        /// <param name="retryCount">Number of times to try the connection</param>
        /// <param name="timeoutSeconds">Number of seconds to wait for the broker to respond</param>
        protected void CreateConnection(int retryCount = 2, int timeoutSeconds = 15)
        {
            if (m_HasConnection)
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

                    IConnectionFactory connectionFactory = new ConnectionFactory(m_BrokerUri);
                    m_Connection = connectionFactory.CreateConnection();
                    m_Connection.RequestTimeout = new TimeSpan(0, 0, timeoutSeconds);
                    m_Connection.Start();

                    m_HasConnection = true;
                    var username = System.Security.Principal.WindowsIdentity.GetCurrent().Name;

                    OnStatusEvent(string.Format("Connected to broker as user {0}: {1}", username, m_BrokerUri));

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

        /// <summary>
        /// Create the message broker communication objects and register the listener function
        /// </summary>
        /// <returns>TRUE for success; FALSE otherwise</returns>
        public bool Init()
        {
            try
            {
                if (!m_HasConnection)
                    CreateConnection();

                if (!m_HasConnection)
                    return false;

                var inputSession = m_Connection.CreateSession();
                m_InputConsumer = inputSession.CreateConsumer(new ActiveMQTopic(m_InputStatusTopicName));
                m_InputConsumer.Listener += OnInputMessageReceived;
                OnStatusEvent("Input listener established for topic '" + m_InputStatusTopicName + "'");

                // topic for commands broadcast to all folder makers
                var broadcastSession = m_Connection.CreateSession();
                m_BroadcastConsumer = broadcastSession.CreateConsumer(new ActiveMQTopic(m_BroadcastTopicName));
                m_BroadcastConsumer.Listener += OnBroadcastReceived;
                OnStatusEvent("Broadcast listener established for topic '" + m_BroadcastTopicName + "'");

                // topic to send status information over
                m_StatusSession = m_Connection.CreateSession();
                m_StatusSender = m_StatusSession.CreateProducer(new ActiveMQTopic(m_OutputStatusTopicName));
                OnStatusEvent("Status sender established for topic '" + m_OutputStatusTopicName + "'");

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

            OnDebugEvent("clsMessageHandler(), Broadcast message received");
            if (BroadcastReceived != null)
            {
                // call the delegate to process the commnd
                BroadcastReceived(textMessage.Text);
            }
            else
            {
                OnDebugEvent("clsMessageHandler().OnBroadcastReceived: No event handlers assigned");
            }
        }

        /// <summary>
        /// Sends a status message
        /// </summary>
        /// <param name="processor"></param>
        /// <param name="message">Outgoing message string</param>
        public void SendMessage(string processor, string message)
        {
            if (!m_IsDisposed)
            {
                var textMessage = m_StatusSession.CreateTextMessage(message);
                textMessage.Properties.SetString("ProcessorName", processor);
                try
                {
                    m_StatusSender.Send(textMessage);
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

        #endregion

        #region "Cleanup"

        /// <summary>
        /// Cleans up a connection after error or when closing
        /// </summary>
        protected void DestroyConnection()
        {
            if (m_HasConnection)
            {
                m_Connection.Dispose();
                m_HasConnection = false;
            }
        }

        /// <summary>
        /// Implements IDisposable interface
        /// </summary>
        public void Dispose()
        {
            if (m_IsDisposed)
                return;

            DestroyConnection();
            m_IsDisposed = true;
        }

        #endregion
    }
}
