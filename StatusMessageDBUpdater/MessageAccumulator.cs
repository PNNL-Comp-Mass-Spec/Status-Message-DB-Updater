// processor message accumulator
// Remembers most recent message each processor
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace StatusMessageDBUpdater {
    class MessageAccumulator {
        // where we accumulate most recent message for each processor
        public IDictionary<string, string> statusList;
        public IDictionary<string, int> changedList;
        public int msgCount = 0;

        public MessageAccumulator() {
            statusList = new Dictionary<string, string>();
            changedList = new Dictionary<string, int>();
        }

        // delegate to be registered with message subscriber 
        // that will be called for each new message that is received
        public void subscriber_OnMessageReceived(string processor, string message) {
            // stuff the message into the accumulator
            // (will overwrite any previous message)
            statusList[processor] = message;
            changedList[processor] = 1;
            msgCount++;
            //Console.WriteLine(processor); // temporary debug
        }

    }
}
