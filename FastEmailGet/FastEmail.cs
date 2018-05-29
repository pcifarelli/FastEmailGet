using System;
using System.IO;
using System.Collections.Generic;
using System.Diagnostics;
using Amazon;
using Amazon.SimpleEmail;
using Amazon.SimpleEmail.Model;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using Amazon.S3;
using Amazon.S3.Model;
using Newtonsoft.Json.Linq;

namespace FastEmailGet
{
    class FastEmailMailbox
    {
        public FastEmailMailbox(string name, string domain, string bucket, string topic)
        {
            Name = name;
            Domain = domain;
            S3Bucket = bucket;
            TopicArn = topic;
        }
        public string Name;
        public string Domain;
        public string S3Bucket;
        public string TopicArn;
    }

    class FastEmail
    {
        private RegionEndpoint endpoint = RegionEndpoint.EUWest1;
        private List<string> rulesets = new List<string> { "default-rule-set" };
        private AmazonSimpleEmailServiceClient sesclient;
        private AmazonSimpleNotificationServiceClient snsclient;
        private bool snsclient_created = false;
        private AmazonSQSClient sqsclient;
        private bool sqsclient_created = false;
        private AmazonS3Client s3client;
        private bool s3client_created = false;
        private Dictionary<string, FastEmailMonitor> monitors;
        public Dictionary<string, FastEmailMailbox> Mailboxes;

        public FastEmail()
        {
            init();
        }

        public FastEmail(Amazon.RegionEndpoint region)
        {
            if (region != null)
                endpoint = region;
            Console.WriteLine($"endpoint: {endpoint}");
            init();
        }

        public FastEmail(List<string> lrulesets)
        {
            rulesets.Clear();
            rulesets = lrulesets;
            init();
        }

        public FastEmail(RegionEndpoint region, string lruleset)
        {
            if (region != null)
                endpoint = region;

            if (lruleset != "")
            {
                rulesets.Clear();
                rulesets.Add(lruleset);
            }
            init();
        }

        public FastEmail(RegionEndpoint region, List<string> lrulesets)
        {
            if (region != null)
                endpoint = region;

            rulesets.Clear();
            rulesets = lrulesets;

            init();
        }

        public bool Exists(string mbx)
        {
            if (Mailboxes.ContainsKey(mbx))
                return true;

            return false;
        }

        public void MonitorEmail(string emailaddr)
        {
            if (Exists(emailaddr) && !monitors.ContainsKey(emailaddr))
            {
                FastEmailMailbox mbx = Mailboxes[emailaddr];
                FastEmailMonitor monitor = new FastEmailMonitor();
                monitor.mbx = mbx;
                monitor.sqsqueue_name = mbx.S3Bucket + "-" + Process.GetCurrentProcess().Id + "-" + System.Environment.MachineName;
                create_sqsclient();
                try
                {
                    // create the queue
                    var sqscreateresponse = sqsclient.CreateQueueAsync(new CreateQueueRequest
                    {
                        QueueName = monitor.sqsqueue_name
                    });

                    CreateQueueResponse sqsresult = sqscreateresponse.Result;
                    monitor.sqsqueue_url = sqsresult.QueueUrl;
                }
                catch (AmazonSQSException e)
                {
                    Console.WriteLine("Exception while creating SQS Queue for {0}: {1}", emailaddr, e.Message);
                }

                // get the queue arn 
                try
                {
                    List<string> attr = new List<string>() { "QueueArn" };
                    var sqsattrresponse = sqsclient.GetQueueAttributesAsync(new GetQueueAttributesRequest
                    {
                        QueueUrl = monitor.sqsqueue_url,
                        AttributeNames = attr
                    });
                    GetQueueAttributesResponse sqsresponse = sqsattrresponse.Result;
                    monitor.sqsqueue_arn = sqsresponse.QueueARN;
                }
                catch (AmazonSQSException e)
                {
                    Console.WriteLine("Exception while getting QueueARN SQS Queue for {0}: {1}", emailaddr, e.Message);
                }

                // add permission
                string perm = @"{
                ""Version"":""2012-10-17"",
                ""Statement"":[
                {
                    ""Sid"":""Policy-"
                + monitor.mbx.TopicArn;
                perm += @""", 
                ""Effect"":""Allow"", 
                ""Principal"":""*"",
                ""Action"":""sqs:SendMessage"",
                ""Resource"":"""
                + monitor.sqsqueue_arn;
                perm += @""", 
                ""Condition"":{ 
                    ""ArnEquals"":{
                        ""aws:SourceArn"":"""
                + monitor.mbx.TopicArn;
                perm += @""" 
                                }
                            }
                        }
                    ]
                }";
                var policy = new Dictionary<string, string>();
                policy.Add("Policy", perm);
                try
                {
                    var qsattrrequest = sqsclient.SetQueueAttributesAsync(new SetQueueAttributesRequest
                    {
                        QueueUrl = monitor.sqsqueue_url,
                        Attributes = policy
                    });
                    var qsattrresponse = qsattrrequest.Result;
                }
                catch (AmazonSQSException e)
                {
                    Console.WriteLine("Exception while adding permission policy to queue for {0}: {1}", emailaddr, e.Message);
                }
                create_snsclient();
                try
                {
                    var snsresponse = snsclient.SubscribeAsync(new SubscribeRequest
                    {
                        Protocol = "sqs",
                        Endpoint = monitor.sqsqueue_arn,
                        TopicArn = monitor.mbx.TopicArn
                    });
                    var subresult = snsresponse.Result;
                    monitor.subscription_arn = subresult.SubscriptionArn;
                }
                catch (AmazonSimpleNotificationServiceException e)
                {
                    Console.WriteLine("Exception while subscribing to queue for {0}: {1}", emailaddr, e.Message);
                }

                monitors.Add(emailaddr, monitor);
            }

        }

        public void UnmonitorEmail(string emailaddr)
        {
            if (monitors.ContainsKey(emailaddr))
            {
                var monitor = monitors[emailaddr];
                try
                {
                    var unsubresponse = snsclient.UnsubscribeAsync(new UnsubscribeRequest
                    {
                        SubscriptionArn = monitor.subscription_arn
                    });
                    var unsubresult = unsubresponse.Result;
                }
                catch (AmazonSimpleNotificationServiceException e)
                {
                    Console.WriteLine("Unable to unsubscripe topic for {0}: {1}", emailaddr, e.Message);
                }
                try
                {
                    var sqsresponse = sqsclient.DeleteQueueAsync(new DeleteQueueRequest
                    {
                        QueueUrl = monitor.sqsqueue_url
                    });

                    DeleteQueueResponse sqsresult = sqsresponse.Result;
                }
                catch (AmazonSQSException e)
                {
                    Console.WriteLine("Unable to delete queue for {0}: {1}", emailaddr, e.Message);
                }
            }
        }

        public string WaitForNextEmail(string emailaddr, int waittimesec)
        {
            string email = "";
            int waittime = waittimesec > 20 ? 20 : waittimesec;

            if (Exists(emailaddr))
            {
                var monitor = monitors[emailaddr];
                do
                {
                    try
                    {
                        var sqsresp = sqsclient.ReceiveMessageAsync(new ReceiveMessageRequest
                        {
                            QueueUrl = monitor.sqsqueue_url,
                            WaitTimeSeconds = waittime,
                            MaxNumberOfMessages = 1
                        });
                        waittimesec -= waittime;
                        waittime = waittimesec > 20 ? 20 : waittimesec;
                        var sqsresult = sqsresp.Result;
                        if (sqsresult.Messages.Count > 0)
                        {
                            if (sqsresult.Messages.Count != 1)
                                Console.WriteLine($"Message Count unexpectedly greater than 1 for {emailaddr}");

                            Amazon.SQS.Model.Message msg = sqsresult.Messages[0];
                            string objectkey = GetObjectKeyFromMessage(msg);
                            create_s3client();
                            try
                            {
                                var s3response = s3client.GetObjectAsync(new GetObjectRequest
                                {
                                    BucketName = monitor.mbx.S3Bucket,
                                    Key = objectkey
                                });
                                var s3result = s3response.Result;
                                using (var strm = s3result.ResponseStream)
                                {
                                    StreamReader reader = new StreamReader(strm);
                                    email = reader.ReadToEnd();
                                }
                            }
                            catch (AmazonS3Exception e)
                            {
                                Console.WriteLine("Exception when trying to download from bucket {0} for {1}: {2}", monitor.mbx.S3Bucket, emailaddr, e.Message);
                            }
                            break;
                        }
                    }
                    catch (AmazonSQSException e)
                    {
                        Console.WriteLine("Exception while waiting for next notification for {0}: {1}", emailaddr, e.Message);
                    }
                } while (waittimesec > 0);
            }
            return email;
        }

        public class FastEmailMonitor
        {
            public FastEmailMailbox mbx;
            public string sqsqueue_name;
            public string sqsqueue_url;
            public string sqsqueue_arn;
            public string subscription_arn;
        }
        private void init()
        {
            try
            {
                sesclient = new AmazonSimpleEmailServiceClient(endpoint);
            }
            catch (AmazonSimpleEmailServiceException e)
            {
                Console.WriteLine("Exception while creating SES Client: {0}", e.Message);
            }
            Mailboxes = new Dictionary<string, FastEmailMailbox>();
            monitors = new Dictionary<string, FastEmailMonitor>();
            GetRules(rulesets);
        }

        private bool create_sqsclient()
        {
            try
            {
                if (!sqsclient_created)
                {
                    sqsclient = new AmazonSQSClient(endpoint);
                    sqsclient_created = true;
                }
            }
            catch (AmazonSQSException e)
            {
                Console.WriteLine("Exception while creating SQS Client: {0}", e.Message);
            }
            return sqsclient_created;
        }

        private bool create_snsclient()
        {
            try
            {
                if (!snsclient_created)
                {
                    snsclient = new AmazonSimpleNotificationServiceClient(endpoint);
                    snsclient_created = true;
                }
            }
            catch (AmazonSimpleNotificationServiceException e)
            {
                Console.WriteLine("Exception while creating SNS Client: {0}", e.Message);
            }
            return snsclient_created;
        }

        private bool create_s3client()
        {
            try
            {
                if (!s3client_created)
                {
                    s3client = new AmazonS3Client(endpoint);
                    s3client_created = true;
                }
            }
            catch (AmazonS3Exception e)
            {
                Console.WriteLine("Exception while creating S3 Client: {0}", e.Message);
            }
            return s3client_created;
        }

        private string GetObjectKeyFromMessage(Amazon.SQS.Model.Message msg)
        {
            JObject notif = JObject.Parse(msg.Body);
            if (notif.ContainsKey("Message"))
            {
                JToken jval = notif.GetValue("Message");
                JObject message = JObject.Parse(jval.ToString());
                if (message.ContainsKey("receipt"))
                {
                    JToken receipt = message.GetValue("receipt");
                    JObject jreceipt = JObject.Parse(receipt.ToString());
                    if (jreceipt.ContainsKey("action"))
                    {
                        JToken action = jreceipt.GetValue("action");
                        JObject jaction = JObject.Parse(action.ToString());
                        if (jaction.ContainsKey("objectKey"))
                        {
                            JToken objkey = jaction.GetValue("objectKey");
                            string s3object_key = objkey.ToString();
                            return s3object_key;
                        }
                    }
                }
                else
                    Console.WriteLine("Didnt find objkey");
            }
            return "";
        }
        private void GetRules(List<string> rulesets)
        {
            foreach (var ruleset in rulesets)
                GetRules(ruleset);
        }

        private void GetRules(string ruleset)
        {
            var response = sesclient.DescribeReceiptRuleSetAsync(new DescribeReceiptRuleSetRequest
            {
                RuleSetName = ruleset
            });

            try
            {
                DescribeReceiptRuleSetResponse result = response.Result;
                List<ReceiptRule> rules = result.Rules;
                foreach (ReceiptRule rule in rules)
                {
                    var recipients = rule.Recipients;
                    foreach (var recipient in recipients)
                    {
                        string[] parts = recipient.Split('@');
                        var actions = rule.Actions;
                        var name = parts[0];
                        var domain = parts[1];
                        string bucket = "";
                        string topic = "";
                        foreach (var action in actions)
                        {
                            try
                            {
                                bucket = action.S3Action.BucketName;
                                topic = action.S3Action.TopicArn;
                            }
                            catch (AmazonSimpleEmailServiceException e)
                            {
                                Console.WriteLine("No Topic found in SES S3Action. Message: '{0}' ", e.Message);
                                if (topic == "")
                                {
                                    try
                                    {
                                        topic = action.SNSAction.TopicArn;
                                    }
                                    catch (AmazonSimpleEmailServiceException e1)
                                    {
                                        Console.WriteLine("No Topic available from SES SNSAction on {0}. Message:'{1}'", recipient, e1.Message);
                                    }
                                }
                            }
                        }
                        if (bucket != "" && topic != "")
                        {
                            var mbx = new FastEmailMailbox(name, domain, bucket, topic);
                            Mailboxes.Add(recipient, mbx);
                        }
                    }
                }
            }
            catch
            {
                Console.WriteLine("Unhandled exception while processing SES mailboxes");
            }
        }
    }
}
