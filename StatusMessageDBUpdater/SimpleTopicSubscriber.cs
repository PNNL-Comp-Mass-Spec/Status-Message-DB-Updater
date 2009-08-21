using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.NMS.ActiveMQ;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS;
using System.Xml;

namespace StatusMessageDBUpdater
{
    // defines format of external functions that can be notified
    public delegate void MessageReceivedDelegate(string processor, string message);
    public delegate void ConnectionExceptionDelegate(Exception e);

    //    public delegate void ExceptionListener(Exception exception);

    public class SimpleTopicSubscriber : IDisposable {
        // list of currently registered external call-back delegates
        public event MessageReceivedDelegate OnMessageReceived;
        public event ConnectionExceptionDelegate OnConnectionException;

        // Objects that NMS library uses to talk to ActiveMQ
        private bool isDisposed = false;
        private string topicName = null;
        private IConnectionFactory connectionFactory;
        private IConnection connection;
        private ISession session;
        private IMessageConsumer consumer;
        public string selector = "";

        public SimpleTopicSubscriber(string topicName, string brokerUri, ref string clientId) {
            InitializeConnection(topicName, brokerUri, clientId);
        }

        public SimpleTopicSubscriber() {
        }

        public void InitializeConnection(string topicName, string brokerUri, string clientId) {
            // create connection to message broker
            this.connectionFactory = new ConnectionFactory(brokerUri);
            this.connection = this.connectionFactory.CreateConnection();
            this.connection.ClientId = clientId;
            this.connection.Start();
            this.connection.ExceptionListener += new ExceptionListener(ExListener);

            // define pub/sub topic that we will be listening to
            this.topicName = topicName;
            this.session = connection.CreateSession();
            ActiveMQTopic topic = new ActiveMQTopic(topicName);

            // create a consumer to actually receive messages
            // from the broker and register our local listener
            if (this.selector == "") {
                this.consumer = this.session.CreateConsumer(topic);  
            } else {
                this.consumer = this.session.CreateConsumer(topic, this.selector);
            }
            this.consumer.Listener += new MessageListener(OnMessage);
        }

        // new messages appear here and are passed on to the delegates (if any)
        public void OnMessage(IMessage message) {
            ITextMessage textMessage = message as ITextMessage;
            String processor = message.Properties.GetString("ProcessorName");

            if (this.OnMessageReceived != null) {
                this.OnMessageReceived(processor, textMessage.Text);
            }
        }

        // connection exceptions appear here and are passed on to the delegates
        public void ExListener(Exception e) {
            if (this.OnConnectionException != null) {
                this.OnConnectionException(e);
            }
        }

        // convenience method for send a topic message via the existing set of 
        // message objects
        public void SendMessage(string topicName, string messageText) {
            ISession session = this.connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
            ITextMessage message = session.CreateTextMessage(messageText);
            IMessageProducer producer = session.CreateProducer(new ActiveMQTopic(topicName));
            producer.Persistent = false;
            producer.Send(message);
        }

        #region IDisposable Members

        public void Dispose() {
            if (!this.isDisposed) {
                this.consumer.Dispose();
                this.session.Dispose();
                this.connection.Dispose();
                this.isDisposed = true;
            }
        }

        #endregion
    }
}
