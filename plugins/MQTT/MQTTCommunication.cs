#region usings
//vvvv related
using System;
using System.ComponentModel.Composition;
using VVVV.PluginInterfaces.V2;
using VVVV.Core.Logging;

//functionality related
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Messages;
#endregion usings

namespace VVVV.Nodes.MQTT
{
    /// <summary>
    /// handling sending and receiving of mqtt-client
    /// including vvvv plugininterfacing
    /// </summary>
    #region PluginInfo
    [PluginInfo(Name = "MQTT", 
                Category = "Network",
                Version = "Client",
                Help = "Client for communicating via the MQTT protocol",
                Tags = "IoT, MQTT", Credits = "M2MQTT m2mqtt.wordpress.com, Jochen Leinberger, explorative-environments.net", 
                Author = "woei",
                Bugs = "receiving delete retained message command",
                AutoEvaluate = true)]
    #endregion PluginInfo
    public class MQTTCommunication : MQTTConnection
    {
        #region pins
        [Input("Topic", DefaultString = "vvvv/topic")]
        public ISpread<string> FInTopic;

        [Input("Message", DefaultString = "hello vvvv")]
        public ISpread<string> FInMessage;

        [Input("Qualiy of Service", DefaultEnumEntry = "QoS_0")]
        public IDiffSpread<QOS> FInQoS;

        [Input("Retained", IsBang = true)]
        ISpread<bool> FInRetained;

        [Input("Do Send", IsBang = true)]
        ISpread<bool> FInSend;

        [Input("Remove Retained", IsBang = true)]
        ISpread<bool> FInRemoveRetained;

        [Input("Receive")]
        ISpread<bool> FInReceive;

        [Output("Topic")]
        public ISpread<string> FOutTopic;

        [Output("Message")]
        public ISpread<string> FOutMessage;

        [Output("Qualiy of Service")]
        public ISpread<QOS> FOutQoS;

        [Output("Is Retained")]
        ISpread<bool> FOutIsRetained;

        [Output("On Data")]
        ISpread<bool> FOutOnData;

        [Output("Publish Queue Count", Visibility = PinVisibility.OnlyInspector)]
        public ISpread<int> FOutboundCount;

        [Output("Message Status")]
        public ISpread<string> FOutMessageStatus;
        #endregion pins

        #region fields
        Queue<string> FMessageStatusQueue = new Queue<string>();

        HashSet<ushort> FPublishStatus = new HashSet<ushort>(); //keeps track received-confirmations of published packets

        Queue<MqttMsgPublishEventArgs> FPacketQueue = new Queue<MqttMsgPublishEventArgs>();  //incoming packets
        HashSet<Tuple<string, QOS>> FSubscriptions = new HashSet<Tuple<string, QOS>>(); //list of current subscriptions
        Dictionary<ushort, Tuple<string, QOS>> FSubscribeStatus = new Dictionary<ushort, Tuple<string, QOS>>(); //matches subscribe commands to packet ids
        Dictionary<ushort, Tuple<string, QOS>> FUnsubscribeStatus = new Dictionary<ushort, Tuple<string, QOS>>(); //matches unsubscribe commands to packet ids
        #endregion fields

        public override void Dispose()
        {
            try
            {
                foreach (var tup in FSubscriptions)
                    FClient.Unsubscribe(new string[] { tup.Item1 });
            }
            catch (Exception e)
            {
                FLogger.Log(e);
            }
            base.Dispose();
        }

        public override void Evaluate(int spreadMax)
        {
            base.Evaluate(spreadMax);

            if ((FClient != null) && FClient.IsConnected)
            {
                HashSet<Tuple<string, QOS>> currentSubscriptions = new HashSet<Tuple<string, QOS>>();
                List<Tuple<string, QOS>> newSubscriptions = new List<Tuple<string, QOS>>();
                for (int i = 0; i < spreadMax; i++)
                {
                    #region sending
                    if (FInSend[i])
                    {
                        if (FInTopic[i].Contains("/#") || FInTopic[i].Contains("/*"))
                        {
                            FMessageStatusQueue.Enqueue("Topic at slice " + i.ToString() + " contains illegal characters for publishing");
                        }
                        else
                        {
                            var packetId = FClient.Publish(FInTopic[i], UTF8Enc.GetBytes(FInMessage[i]), (byte)FInQoS[i], FInRetained[i]);
                            FPublishStatus.Add(packetId);
                        }

                    }
                    if (FInRemoveRetained[i])
                        FClient.Publish(FInTopic[i], new byte[] { }, (byte)0, true);
                    #endregion sending

                    //subscription and unsubscription has to be handled outside this loop
                    //unsubscription has to be triggered first (matters in the case of same topic with different qos)
                    #region receiving
                    if (FInReceive[i])
                    {
                        var tup = new Tuple<string, QOS>(FInTopic[i], FInQoS[i]);
                        currentSubscriptions.Add(tup);
                        
                        if (!FSubscriptions.Remove(tup))
                            newSubscriptions.Add(tup);
                    }
                    #endregion receiving
                }

                #region unsubscribe
                try
                {
                    if (FSubscriptions.Count > 0)
                    {
                        foreach (var tuple in FSubscriptions)
                        {
                            var unsubscribeId = FClient.Unsubscribe(new string[] { tuple.Item1 });
                            FUnsubscribeStatus.Add(unsubscribeId, tuple);
                        }
                    }
                    FSubscriptions = new HashSet<Tuple<string, QOS>>(currentSubscriptions);
                }
                catch (Exception e)
                {
                    FLogger.Log(e);
                    foreach (var s in currentSubscriptions)
                        FSubscriptions.Add(s);
                }
                #endregion unsubscribe

                #region subscribe
                if (FNewSession)
                {
                    newSubscriptions.AddRange(FSubscriptions);
                    FNewSession = false;
                }
                foreach (var subs in newSubscriptions)
                {
                    try
                    {
                        var subscribeId = FClient.Subscribe(new string[] { subs.Item1 }, new byte[] { (byte)subs.Item2 });
                        FSubscribeStatus.Add(subscribeId, subs);
                    }
                    catch
                    {
                        FLogger.Log(LogType.Warning, string.Format("couldn't subscribe to {0} with qos {1}", subs.Item1, subs.Item2));
                    }
                }
                #endregion subscribe
            }

            if (FMessageStatusQueue.Count > 0)
            {
                FOutMessageStatus.AssignFrom(FMessageStatusQueue.ToArray());
                FMessageStatusQueue.Clear();
            }

            FOutTopic.AssignFrom(FPacketQueue.Select(x => x.Topic).ToArray());
            FOutMessage.AssignFrom(FPacketQueue.Select(x => UTF8Enc.GetString(x.Message)));
            FOutQoS.AssignFrom(FPacketQueue.Select(x => (QOS)x.QosLevel));
            FOutIsRetained.AssignFrom(FPacketQueue.Select(x => x.Retain));
            FOutOnData[0] = FPacketQueue.Count > 0;
            FPacketQueue.Clear();

            FOutboundCount[0] = FPublishStatus.Count;
        }

        #region events
        /// <summary>
        /// confirmation of the broker that a packet was successfully transmitted
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        public override void FClient_MqttMsgPublished(object sender, MqttMsgPublishedEventArgs e)
        {
            FMessageStatusQueue.Enqueue(PrependTime("published message with packet ID " + e.MessageId));
            FPublishStatus.Remove(e.MessageId);
        }

        /// <summary>
        /// passes packets received by the mqtt-client
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e">packet data</param>
        public override void FClient_MqttMsgPublishReceived(object sender, MqttMsgPublishEventArgs e)
        {
            FPacketQueue.Enqueue(e);
            FMessageStatusQueue.Enqueue(PrependTime("received topic " + e.Topic));
        }

        /// <summary>
        /// broker acknowledges a subscription
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        public override void FClient_MqttMsgSubscribed(object sender, MqttMsgSubscribedEventArgs e)
        {
            var issued = FSubscribeStatus[e.MessageId];
            FMessageStatusQueue.Enqueue(PrependTime("subscribed to " + issued.Item1));
            FSubscribeStatus.Remove(e.MessageId);
        }

        /// <summary>
        /// broker acknowledges an unsubscription
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        public override void FClient_MqttMsgUnsubscribed(object sender, MqttMsgUnsubscribedEventArgs e)
        {
            var issued = FUnsubscribeStatus[e.MessageId];
            FMessageStatusQueue.Enqueue(PrependTime("unsubscribed from " + issued.Item1));
            FUnsubscribeStatus.Remove(e.MessageId);
        }
        #endregion events
    }
}
