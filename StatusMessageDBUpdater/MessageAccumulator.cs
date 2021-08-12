using System.Collections.Generic;

namespace StatusMessageDBUpdater
{

    /// <summary>
    /// Processor message accumulator
    /// Remembers most recent message each processor
    /// </summary>
    internal class MessageAccumulator
    {
        /// <summary>
        /// Tracks most recent message for each processor
        /// </summary>
        public readonly Dictionary<string, string> StatusList;

        /// <summary>
        /// Tracks names of processors with a message in StatusList
        /// </summary>
        public readonly SortedSet<string> ChangedList;

        /// <summary>
        /// Constructor
        /// </summary>
        public MessageAccumulator()
        {
            StatusList = new Dictionary<string, string>();
            ChangedList = new SortedSet<string>();
        }

        // Delegate to be registered with message subscriber
        // that will be called for each new message that is received
        public void Subscriber_OnMessageReceived(string processor, string message)
        {
            // Stuff the message into the accumulator
            // (will overwrite any previous message)
            StatusList[processor] = message;

            if (!ChangedList.Contains(processor))
                ChangedList.Add(processor);

        }

    }
}
