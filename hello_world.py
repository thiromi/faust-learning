import faust

from faust import Stream

from models import Customer, CustomerEvent

app = faust.App('hello-world', broker='kafka://localhost:29092', store="memory://")


customers = app.Table('customers', partitions=1, value_type=Customer)

customer_became_lead_topic = app.topic('customer_became_lead', partitions=1, value_type=CustomerEvent)
customer_subscribed_topic = app.topic('customer_subscribed', partitions=1, value_type=CustomerEvent)
customer_cancelled_topic = app.topic('customer_cancelled', partitions=1, value_type=CustomerEvent)


@app.agent(customer_became_lead_topic)
async def customer_became_lead(events: Stream[CustomerEvent]):
    async for event in events:
        try:
            customer = customers[f'{event.business_unit}_{event.customer_id}']
            customer.updated_at = event.timestamp
            customer.status = 'lead'
        except KeyError:
            customer = Customer(
                id=event.customer_id,
                business_unit=event.business_unit,
                created_at=event.timestamp,
                updated_at=event.timestamp,
            )

        customers[customer.key] = customer
        print(customer)


@app.agent(customer_subscribed_topic)
async def customer_subscribed(events: Stream[CustomerEvent]):
    async for event in events:
        try:
            customer = customers[f'{event.business_unit}_{event.customer_id}']
            customer.status = 'active'
            customer.subscribed_at = event.timestamp
            customer.updated_at = event.timestamp
        except KeyError:
            customer = Customer(
                id=event.customer_id,
                business_unit=event.business_unit,
                status='active',
                subscribed_at=event.timestamp,
                updated_at=event.timestamp,
            )

        customers[customer.key] = customer
        print(customer)


@app.agent(customer_cancelled_topic)
async def customer_cancelled(events: Stream[CustomerEvent]):
    async for event in events:
        try:
            customer = customers[f'{event.business_unit}_{event.customer_id}']
            customer.status = 'former'
            customer.cancelled_at = event.timestamp
        except KeyError:
            customer = Customer(
                id=event.customer_id,
                business_unit=event.business_unit,
                status='active',
                cancelled_at=event.timestamp,
                updated_at=event.timestamp,
            )

        customers[customer.key] = customer
        print(customer)


@app.page('/customers')
async def list_customers(self, request):
    return self.json(list(customers.values()))


@app.page('/customers')
async def list_customers(self, request):
    return self.json(list(customers.values()))


if __name__ == '__main__':
    app.main()
