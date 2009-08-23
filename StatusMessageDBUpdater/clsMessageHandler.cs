using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.NMS;
using Apache.NMS.ActiveMQ;
using Apache.NMS.ActiveMQ.Commands;

namespace StatusMessageDBUpdater {
    // received commands are sent to a delegate function with this signature
    public delegate void MessageProcessorDelegate(string cmdText);

    class clsMessageHandler : IDisposable {
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
        public event MessageProcessorDelegate InputMessageReceived;
        public event MessageProcessorDelegate BroadcastReceived;
        #endregion

        #region "Properties"
        public string MgrName {
            set { m_MgrName = value; }
        }

        public string BrokerUri {
            get { return m_BrokerUri; }
            set { m_BrokerUri = value; }
        }

        public string InputStatusTopicName {
            get { return m_InputStatusTopicName; }
            set { m_InputStatusTopicName = value; }
        }

        public string BroadcastTopicName {
            get { return m_BroadcastTopicName; }
            set { m_BroadcastTopicName = value; }
        }

        public string OutputStatusTopicName {
            get { return m_OutputStatusTopicName; }
            set { m_OutputStatusTopicName = value; }
        }
        #endregion

        #region "Methods"
        /// <summary>
        /// create set of NMS connection objects necessary to talk to the ActiveMQ broker
        /// </summary>
        protected void CreateConnection() {
            if (m_HasConnection) return;
            try {
                IConnectionFactory connectionFactory = new ConnectionFactory(this.m_BrokerUri);
                this.m_Connection = connectionFactory.CreateConnection();
                this.m_Connection.Start();

                this.m_HasConnection = true;
                // temp debug
                // Console.WriteLine("--- New connection made ---" + Environment.NewLine); //+ e.ToString()
                string msg = "Connected to broker";
///                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
            } catch (Exception Ex) {
                // we couldn't make a viable set of connection objects 
                // - this has "long day" written all over it,
                // but we don't have to do anything specific at this point (except eat the exception)

                // Console.WriteLine("=== Error creating connection ===" + Environment.NewLine); //+ e.ToString() // temp debug
                string msg = "Exception creating broker connection";
///                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, Ex);
            }
        }	// End sub

        /// <summary>
        /// Create the message broker communication objects and register the listener function
        /// </summary>
        /// <returns>TRUE for success; FALSE otherwise</returns>
        public bool Init() {
            try {
                if (!m_HasConnection) CreateConnection();
                if (!m_HasConnection) return false;

                // topic for input status messages
                ISession inputSession = m_Connection.CreateSession();
                m_InputConsumer = inputSession.CreateConsumer(new ActiveMQTopic(this.m_InputStatusTopicName));
                m_InputConsumer.Listener += new MessageListener(OnInputMessageReceived);
///                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Inut listener established");

                // topic for commands broadcast to all folder makers
                ISession broadcastSession = m_Connection.CreateSession();
                m_BroadcastConsumer = broadcastSession.CreateConsumer(new ActiveMQTopic(this.m_BroadcastTopicName));
                m_BroadcastConsumer.Listener += new MessageListener(OnBroadcastReceived);
///                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Broadcast listener established");

                // topic to send status information over
                this.m_StatusSession = m_Connection.CreateSession();
                this.m_StatusSender = m_StatusSession.CreateProducer(new ActiveMQTopic(m_OutputStatusTopicName));
///                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Status sender established");

                return true;
            } catch (Exception Ex) {
                string msg = "Exception while initializing messages sessiions";
///                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, Ex);
                DestroyConnection();
                return false;
            }
        }	// End sub

        /// <summary>
        /// Command listener function. Received commands will cause this to be called
        ///	and it will trigger an event to pass on the command to all registered listeners
        /// </summary>
        /// <param name="message">Incoming message</param>
        private void OnInputMessageReceived(IMessage message) {
            ITextMessage textMessage = message as ITextMessage;
            string Msg = "clsMessageHandler(), Command message received";
///            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg);
            if (this.InputMessageReceived != null) {
                // call the delegate to process the commnd
                Msg = "clsMessageHandler().OnInputMessageReceived: At lease one event handler assigned";
///                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg);
                this.InputMessageReceived(textMessage.Text);
            } else {
                Msg = "clsMessageHandler().OnInputMessageReceived: No event handlers assigned";
///                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg);
            }
        }	// End sub

        /// <summary>
        /// Broadcast listener function. Received Broadcasts will cause this to be called
        ///	and it will trigger an event to pass on the command to all registered listeners
        /// </summary>
        /// <param name="message">Incoming message</param>
        private void OnBroadcastReceived(IMessage message) {
            ITextMessage textMessage = message as ITextMessage;
            string Msg = "clsMessageHandler(), Broadcast message received";
///           clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg);
            if (this.BroadcastReceived != null) {
                // call the delegate to process the commnd
                Msg = "clsMessageHandler().OnBroadcastReceived: At lease one event handler assigned";
///                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg);
                this.BroadcastReceived(textMessage.Text);
            } else {
                Msg = "clsMessageHandler().OnBroadcastReceived: No event handlers assigned";
///               clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg);
            }
        }	// End sub

        /// <summary>
        /// Sends a status message
        /// </summary>
        /// <param name="message">Outgoing message string</param>
        public void SendMessage(string message) {
            if (!this.m_IsDisposed) {
                ITextMessage textMessage = this.m_StatusSession.CreateTextMessage(message);
                this.m_StatusSender.Send(textMessage);
            } else {
                throw new ObjectDisposedException(this.GetType().FullName);
            }
        }	// End sub
        #endregion

        #region "Cleanup"
        /// <summary>
        /// Cleans up a connection after error or when closing
        /// </summary>
        protected void DestroyConnection() {
            if (m_HasConnection) {
                this.m_Connection.Dispose();
                this.m_HasConnection = false;
                string msg = "Message connection closed";
 ///               clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
            }
        }	// End sub

        /// <summary>
        /// Implements IDisposable interface
        /// </summary>
        public void Dispose() {
            if (!this.m_IsDisposed) {
                this.DestroyConnection();
                this.m_IsDisposed = true;
            }
        }	// End sub

        #endregion
    }	// End class    }
}
