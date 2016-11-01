using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
        private string m_BrokerUri = null;
        private string m_InputStatusTopicName = null;
        private string m_BroadcastTopicName = null;
        private string m_OutputStatusTopicName = null;
        private string m_MgrName = null;

        private IConnection m_Connection;
        private ISession m_StatusSession;
        private IMessageProducer m_StatusSender;
        private IMessageConsumer m_InputConsumer;
        private IMessageConsumer m_BroadcastConsumer;

        private bool m_IsDisposed = false;
        private bool m_HasConnection = false;
        #endregion

        #region "Events"
        public event MessageReceivedDelegate InputMessageReceived;
        public event MessageProcessorDelegate BroadcastReceived;
        #endregion

        #region "Properties"
        public string MgrName
        {
            set { m_MgrName = value; }
        }

        public string BrokerUri
        {
            get { return m_BrokerUri; }
            set { m_BrokerUri = value; }
        }

        public string InputStatusTopicName
        {
            get { return m_InputStatusTopicName; }
            set { m_InputStatusTopicName = value; }
        }

        public string BroadcastTopicName
        {
            get { return m_BroadcastTopicName; }
            set { m_BroadcastTopicName = value; }
        }

        public string OutputStatusTopicName
        {
            get { return m_OutputStatusTopicName; }
            set { m_OutputStatusTopicName = value; }
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
                IConnectionFactory connectionFactory = new ConnectionFactory(this.m_BrokerUri);
                this.m_Connection = connectionFactory.CreateConnection();
                this.m_Connection.RequestTimeout = new System.TimeSpan(0, 0, 15);
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
            }
        }	// End sub

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
                ISession inputSession = m_Connection.CreateSession();
                m_InputConsumer = inputSession.CreateConsumer(new ActiveMQTopic(this.m_InputStatusTopicName));
                m_InputConsumer.Listener += new MessageListener(OnInputMessageReceived);
                mainLog.Info("Input listener established for topic '" + m_InputStatusTopicName + "'");

                // topic for commands broadcast to all folder makers
                ISession broadcastSession = m_Connection.CreateSession();
                m_BroadcastConsumer = broadcastSession.CreateConsumer(new ActiveMQTopic(this.m_BroadcastTopicName));
                m_BroadcastConsumer.Listener += new MessageListener(OnBroadcastReceived);
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
        }	// End sub

        /// <summary>
        /// Command listener function. Received commands will cause this to be called
        ///	and it will trigger an event to pass on the command to all registered listeners
        /// </summary>
        /// <param name="message">Incoming message</param>
        private void OnInputMessageReceived(IMessage message)
        {
            ITextMessage textMessage = message as ITextMessage;
            String processor = message.Properties.GetString("ProcessorName");
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
            ITextMessage textMessage = message as ITextMessage;
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
        }	// End sub

        /// <summary>
        /// Sends a status message
        /// </summary>
        /// <param name="message">Outgoing message string</param>
        public void SendMessage(string processor, string message)
        {
            if (!this.m_IsDisposed)
            {
                ITextMessage textMessage = this.m_StatusSession.CreateTextMessage(message);
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
        }	// End sub
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
        }	// End sub

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
        }	// End sub

        #endregion
    }	// End class    }
}
