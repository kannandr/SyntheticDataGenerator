
import random
import sys
import uuid

import datetime

from confluent_kafka import Producer

from faker import Faker

fake = Faker('en_US')


if __name__ == '__main__':
    if len(sys.argv) != 4:
        sys.stderr.write('Usage: %s <bootstrap-brokers> <topic> <num msgs>\n' % sys.argv[0])
        sys.exit(1)

    broker = sys.argv[1]
    topic = sys.argv[2]
    total_msg = sys.argv[3]
    
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
    num_msg = int(total_msg)

    # Get current start date to measure data generation time
    ct1 = datetime.datetime.today()
    print("Start time:", ct1)

    # Initialize variables
    line1=0

    # Generate Packet
    fake_data = {}
    for i in range(0, num_msg):
        gender = fake.random_element(elements=('M', 'F'))

        if(gender == 'M'):
            first_name = fake.first_name_male()
        else:
            first_name = fake.first_name_female()

        # fake_data={}
        customer_id = str(uuid.uuid4())
        first_name = first_name
        last_name = fake.last_name()
        address = fake.street_address()
        city = fake.city()
        state = fake.state_abbr()
        zip = fake.zipcode()
        email = fake.ascii_free_email()
        gender = gender
        pet_type = fake.random_element(elements=('Dog', 'Fish', 'Cat', 'Reptile', 'Ferret', 'Hamster', 'Bird', 'Frog', 'Rabbit', 'Spider'))
        last_visit = fake.past_date().strftime("%Y%m%d")
        last_store = fake.random_element(elements=('East Fabian', 'Gerholdchester', 'Wintontown', 'New Candishaven', 'Roobfurt', 'Simonemouth'))
        number_items_purchased = str(fake.random_int(min=1, max=30))
        lifetime_purchase_amount = str(random.uniform(1.0, 100.8))
        grooming_services = str(fake.boolean())
        organic_food = str(fake.boolean())
        vet_referral = str(fake.boolean())

        # debug 
        # print(col_data)

        # Generate the payload
        line =  '{ "schema": { "type": "struct", "fields": [ { "type": "string", "optional": false, "field": "id" }, ' + \
                '{ "type": "string", "optional": false, "field": "first_name" }, { "type": "string", "optional": false, "field": "last_name" }, ' + \
                '{ "type": "string", "optional": false, "field": "address" }, { "type": "string", "optional": false, "field": "city" }, ' + \
                '{ "type": "string", "optional": false, "field": "state" }, { "type": "string", "optional": false, "field": "zip" }, ' + \
                '{ "type": "string", "optional": false, "field": "email" }, { "type": "string", "optional": false, "field": "gender" }, ' + \
                '{ "type": "string", "optional": false, "field": "pet_type" }, { "type": "string", "optional": false, "field": "last_visit" }, ' + \
                '{ "type": "string", "optional": false, "field": "last_store" }, { "type": "int32", "optional": false, "field": "number_items_purchased" }, ' + \
                '{ "type": "float", "optional": false, "field": "lifetime_purchase_amount" }, { "type": "string", "optional": false, "field": "grooming_services" }, ' + \
                '{ "type": "string", "optional": false, "field": "organic_food" }, { "type": "string", "optional": false, "field": "vet_referral" } ] }, ' + \
                '"payload": { "id": "' + customer_id + '", "first_name": "' + first_name + '", "last_name": "' + last_name + '", ' + \
                '"address": "' + address + '", "city": "' + city + '", "state": "' + state + '", ' + \
                '"zip": "' + zip + '", "email": "' + email + '", "gender": "' + gender + '", "pet_type": "' + pet_type + '", ' + \
                '"last_visit": "' + last_visit + '", "last_store": "' + last_store + '", "number_items_purchased": ' + number_items_purchased + \
                ', "lifetime_purchase_amount": ' + lifetime_purchase_amount + ', "grooming_services": "' + grooming_services + '", ' + \
                '"organic_food": "' + organic_food + '", "vet_referral": "' + vet_referral + '" } }'
    
        try:
            # Produce line (without newline)
            p.produce(topic, line.rstrip(), callback=delivery_callback)

        except BufferError:
            pass

        p.poll(0)

        print(str(i) + " " + customer_id)

    ct2 = datetime.datetime.today()
    print("Start time:", ct1)
    print("End time:", ct2)
    print("Total time taken to produce message:", ct2-ct1)

    # Wait until all messages have been delivered
    sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
    p.flush()