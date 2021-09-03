from datetime import datetime
from typing import Optional

import faust


# to use the default JSON serializer (tested w/o schema registry), the type should be of faust.Record
class Customer(faust.Record, coerce=True):
    id: int
    business_unit: str
    status: str = 'lead'
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    subscribed_at: Optional[datetime] = None
    cancelled_at: Optional[datetime] = None

    @property
    def key(self):
        return f'{self.business_unit}_{self.id}'


class CustomerEvent(faust.Record, validation=True):
    customer_id: int
    business_unit: str
    timestamp: datetime