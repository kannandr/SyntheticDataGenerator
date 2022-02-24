import sys
from datetime import datetime

from confluent_kafka import Producer
#FOR AUTO DB ID CREATING:

import uuid
if __name__ == '__main__':
    if len(sys.argv) != 4:
        sys.stderr.write('Usage: %s <bootstrap-brokers> <topic>\n' % sys.argv[0])
        sys.exit(1)

    broker = sys.argv[1]
    topic = sys.argv[2]
    total_msg=sys.argv[3]
    schema_part1='{"schema":{"type":"struct","fields":[{"type":"string","optional":false,"field":"audience_key"},{"type": "array","items": {"type": "string"},"optional": false,"field": "anonymous_identifiers"},'
    type_=['string', 'int32',  'boolean','float','double']

    
    values1=["String_Value_1",10,'true',1.12345,5.4321,["d1","d2","d3","d4"]]
    values2=["String_Value_2",11,'false',2.212345,6.5321,["c1","c2","c3"]]
    values3=["String_Value_3",12,'true',3.312345,7.6321,["b1","b2"]]
    values4=["String_Value_4",13,'false',4.412345,8.7321,["a1"]]
    values5=["String_Value_5",14,'true',5.512345,9.48321,["e1","e2","e3","e4","e5"]]
    values6=["String_Value_6",14,'false',6.612345,10.8321,["f1","f2","f3","f4","f6"]]
    values7=["String_Value_7",14,'true',7.712345,11.9321,["g1","g2","g3","g4","g7"]]
    values8=["String_Value_8",14,'false',8.812345,12.10321,["h1","h2","h3","h4"]]
    values9=["String_Value_9",14,'true',9.912345,13.1121,["i1","i2","i3","i4"]]
    values10=["String_Value_10",14,'false',10.102345,14.12321,["j1","j2","j3","j4"]]
    values=[values1,values2,values3,values4,values5,values6,values7,values8,values9,values10]
    payload_size=500
    iterate=0

    for i in range(payload_size-2):
        if (iterate==5):
            iterate=0
        c=type_[iterate]
        iterate=iterate+1
        field_='{"type":"'+c+'","optional":false,"field":"col_'+str(i)+'"},'
        schema_part1=schema_part1+field_

    schema_part1=schema_part1[:-1]
    

    # Producer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {'bootstrap.servers': broker}

    # Create Producer instance
    p = Producer(**conf)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).

    def delivery_callback(err, msg):
        
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            pass
            #sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
             #                (msg.topic(), msg.partition(), msg.offset()))

    # Read lines from stdin, produce each line to Kafka
    msg_range=int(total_msg)

    ct1 = datetime.now()
    print("Start time:-", ct1)
    line1=0
    #dbid=str(ct1.date())+str(ct1.minute)+
    #dbid=datetime.now().strftime('%H:%M:%S.%f')
    dbid=str(uuid.uuid1())
    dbid=dbid+dbid
    dbid=dbid[:48]
    while (line1!=msg_range):


#       #
        iterate=0
        rand_anon_ids=(i+13)%9
        schema_part2=']},"payload":{"audience_key":"'+str(dbid)+'-'+str(line1)+'","anonymous_identifiers":'+str(values[rand_anon_ids][5]).replace("'",'"')+','
        a2=""
        for i in range(payload_size-2):
        
            rand_str=(i+3)%6
            rand_int=(i+5)%7
            rand_bool=(i+7)%8
            rand_fl=(i+9)%9
            rand_dub=(i+11)%5
            
            if iterate==0:
                c=values[rand_str][iterate]
                v='"col_'+str(i)+'":"'+str(c)+'",'
            if iterate==1:
                c=values[rand_int][iterate]
                v='"col_'+str(i)+'":'+str(c)+','

            if iterate==2:
                c=values[rand_bool][iterate]
                v='"col_'+str(i)+'":'+str(c)+','

            if iterate==3:
                c=values[rand_fl][iterate]
                v='"col_'+str(i)+'":'+str(c)+','

            if iterate==4:
                c=values[rand_dub][iterate]
                v='"col_'+str(i)+'":'+str(c)+','
            a2=a2+v
            iterate=iterate+1
            
            if (iterate==5):
                iterate=0
            

            
            
        
        schema_part3=a2[:-1]

        schema_part4='}}'
        line=schema_part1+schema_part2+schema_part3+schema_part4

        #print("LINE in sys stdin:",line)
        #line=
        try:
            # Produce line (without newline)
            p.produce(topic, line.rstrip(), callback=delivery_callback)
            line1=line1+1

        except BufferError:
            pass
            #sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                             #len(p))

        # Serve delivery callback queue.
        # NOTE: Since produce() is an asynchronous API this poll() call
        #       will most likely not serve the delivery callback for the
        #       last produce()d message.
        p.poll(0)
    ct2 = datetime.now()
    print("Start time:-", ct1)
    print("ID VALUE - ",dbid)
    print("End time:-", ct2)
    print("Total message produced:",line1)
    print("Total TIME TAKEN TO PRODUCE MESSAage:",ct2-ct1)
    # Wait until all messages have been delivered
    sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
    p.flush()

