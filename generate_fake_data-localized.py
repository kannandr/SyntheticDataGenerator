import random
import sys
import uuid

from faker import Faker

fake = Faker('ja_JP')

def rows_data(x):
    
    # Print headers
    print('Customer ID|First Name|Last Name|Address|City|Zip|Email|Gender|Pet Type|Last Visit|Last Store|Number Items Purchased|Lifetime Purchase Amount|Grooming Services|Organic Food|Vet Referral')

    # Generate fake data
    fake_data = {}
    for i in range(0, x):
        gender = fake.random_element(elements=('M', 'F'))

        if(gender == 'M'):
            first_name = fake.first_name_male()
        else:
            first_name = fake.first_name_female()

        fake_data[i]={}
        fake_data[i]['customer_id'] = str(uuid.uuid4())
        fake_data[i]['first_name'] = first_name
        fake_data[i]['last_name'] = fake.last_name()
        fake_data[i]['address'] = fake.street_address()
        fake_data[i]['city'] = fake.city()
        fake_data[i]['zip'] = fake.zipcode()
        fake_data[i]['email'] = fake.ascii_free_email()
        fake_data[i]['gender'] = gender
        fake_data[i]['pet_type'] = fake.random_element(elements=('Dog', 'Fish', 'Cat', 'Reptile', 'Ferret', 'Hamster'))
        fake_data[i]['last_visit'] = fake.past_date().strftime("%Y%m%d")
        fake_data[i]['last_store'] = fake.random_element(elements=('East Fabian', 'Gerholdchester', 'Wintontown', 'New Candishaven', 'Roobfurt', 'Simonemouth'))
        fake_data[i]['number_items_purchased'] = fake.random_int(min=1, max=30)
        fake_data[i]['lifetime_purchase_amount'] = random.uniform(1.0, 100.8)
        fake_data[i]['grooming_services'] = fake.boolean()
        fake_data[i]['organic_food'] = fake.boolean()
        fake_data[i]['vet_referral'] = fake.boolean()

        # print('|'.join(map(str,fake_data[i].values())))
        print(fake_data.values())

def main():
    # Print error if number of rows not supplied
    if len(sys.argv) != 2:
        sys.stderr.write('Usage: %s <num of rows to generate>\n' % sys.argv[0])
        sys.exit(1)

    rows_to_generate = int(sys.argv[1])
    rows_data(rows_to_generate)

main()