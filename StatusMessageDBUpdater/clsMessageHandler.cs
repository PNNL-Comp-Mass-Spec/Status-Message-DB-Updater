using System;
using Apache.NMS;
using Apache.NMS.ActiveMQ;
using Apache.NMS.ActiveMQ.Commands;
using log4net;

namespace StatusMessageDBUpdater
{
    // received input messages are sent to a delegate function with this signature
    public delegate void MessageReceivedDelegate(string processor, string message);

    // received commands are sent to a delegate function with this signature
    public delegate void MessageProcessorDelegate(string cmdText);

    class clsMessageHandler : IDisposable
    {
        private static readonly ILog mainLog = LogManager.GetLogger("MainLog");

        #region "Class variables"
        private string m_BrokerUri;
        private string m_InputStatusTopicName;
        private string m_BroadcastTopicName;
        private string m_OutputStatusTopicName;
        private string m_MgrName;

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
        public string MgrName
        {
            set => m_MgrName = value;
        }

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
        /// create set of NMS connection objects necessary to talk to the ActiveMQ broker
        /// </summary>
        protected void CreateConnection()
        {
            if (m_HasConnection) return;
            try
            {
                // Broker URI should be in the form
                // tcp://Proto-7.pnl.gov:61616
                //  or
                // failover:(tcp://Proto-7.pnl.gov:61616,tcp://proto-4.pnl.gov:61616)

                IConnectionFactory connectionFactory = new ConnectionFactory(this.m_BrokerUri);
                this.m_Connection = connectionFactory.CreateConnection();
                this.m_Connection.RequestTimeout = new System.TimeSpan(0, 0, 20);
                this.m_Connection.Start();

                this.m_HasConnection = true;
                // temp debug
                // Console.WriteLine("--- New connection made ---" + Environment.NewLine); //+ e.ToString()
                mainLog.Info("Connected to message broker");
                mainLog.Info(" ... " + m_BrokerUri);
            }
            catch (Exception ex)
            {
                // we couldn't make a viable set of connection objects 
                // - this has "long day" written all over it,
                // but we don't have to do anything specific at this point (except eat the exception)
                mainLog.Error("Exception creating message broker connection to " + m_BrokerUri);
                mainLog.Error(ex.Message);
                mainLog.Error(PRISM.clsStackTraceFormatter.GetExceptionStackTrace(ex));
            }
        }

        /// <summary>
        /// Create the message broker communication objects
        /// </summary>
        /// <returns>TRUE for success; FALSE otherwise</returns>
        public bool Init()
        {
            try
            {
                if (!m_HasConnection) CreateConnection();
                if (!m_HasConnection) return false;

                // topic for input status messages
                var inputSession = m_Connection.CreateSession();
                m_InputConsumer = inputSession.CreateConsumer(new ActiveMQTopic(this.m_InputStatusTopicName));
                m_InputConsumer.Listener += OnInputMessageReceived;
                mainLog.Info("Input listener established for topic '" + m_InputStatusTopicName + "'");

                // topic for commands broadcast to all folder makers
                var broadcastSession = m_Connection.CreateSession();
                m_BroadcastConsumer = broadcastSession.CreateConsumer(new ActiveMQTopic(this.m_BroadcastTopicName));
                m_BroadcastConsumer.Listener += OnBroadcastReceived;
                mainLog.Info("Broadcast listener established for topic '" + m_BroadcastTopicName + "'");

                // topic to send status information over
                this.m_StatusSession = m_Connection.CreateSession();
                this.m_StatusSender = m_StatusSession.CreateProducer(new ActiveMQTopic(m_OutputStatusTopicName));
                mainLog.Info("Status sender established for topic '" + m_OutputStatusTopicName + "'");

                return true;
            }
            catch (Exception ex)
            {
                mainLog.Error("Exception while initializing message sessions: " + ex.Message);
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
            var textMessage = message as ITextMessage;
            var processor = message.Properties.GetString("ProcessorName");
            if (this.InputMessageReceived != null)
            {
                this.InputMessageReceived(processor, textMessage.Text);
            }
        }

        /// <summary>
        /// Broadcast listener function. Received Broadcasts will cause this to be called
        ///	and it will trigger an event to pass on the command to all registered listeners
        /// </summary>
        /// <param name="message">Incoming message</param>
        private void OnBroadcastReceived(IMessage message)
        {
            var textMessage = message as ITextMessage;
            mainLog.Debug("clsMessageHandler(), Broadcast message received");
            if (this.BroadcastReceived != null)
            {
                // call the delegate to process the commnd
                this.BroadcastReceived(textMessage.Text);
            }
            else
            {
                mainLog.Debug("clsMessageHandler().OnBroadcastReceived: No event handlers assigned");
            }
        }

        /// <summary>
        /// Sends a status message
        /// </summary>
        /// <param name="message">Outgoing message string</param>
        public void SendMessage(string processor, string message)
        {
            if (!this.m_IsDisposed)
            {
                var textMessage = this.m_StatusSession.CreateTextMessage(message);
                textMessage.Properties.SetString("ProcessorName", processor);
                try
                {
                    this.m_StatusSender.Send(textMessage);
                }
                catch
                {
                    // Do nothing
                }
            }
            else
            {
                throw new ObjectDisposedException(this.GetType().FullName);
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
                this.m_Connection.Dispose();
                this.m_HasConnection = false;
            }
        }

        /// <summary>
        /// Implements IDisposable interface
        /// </summary>
        public void Dispose()
        {
            if (!this.m_IsDisposed)
            {
                this.DestroyConnection();
                this.m_IsDisposed = true;
            }
        }

        #endregion
    }
}
