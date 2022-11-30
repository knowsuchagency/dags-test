from pynamodb.attributes import (
    UTCDateTimeAttribute,
    UnicodeAttribute,
    NumberAttribute,
    UnicodeSetAttribute,
)
from pynamodb.models import Model


class DataDiffResult(Model):
    """
    The result of diffing two tables via datadiff.
    """

    class Meta:
        table_name = "phoenix_datadiff_results"
        region = "us-east-1"
        billing_mode = "PAY_PER_REQUEST"

    date = UTCDateTimeAttribute(hash_key=True)
    table = UnicodeAttribute(range_key=True)
    source_schema = UnicodeAttribute()
    target_schema = UnicodeAttribute()
    plus_rows = UnicodeSetAttribute()
    minus_rows = UnicodeSetAttribute()
    plus = NumberAttribute()
    minus = NumberAttribute()
    exception = UnicodeAttribute(null=True)

    @classmethod
    def update_table_name(cls, table_name: str):
        cls.Meta.table_name = table_name
