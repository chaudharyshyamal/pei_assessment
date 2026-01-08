from pyspark.sql.types import StructType
import utils.schemas as schema


def test_raw_schema_definition():
    assert isinstance(schema.products_schema, StructType)
    assert len(schema.products_schema.fields) > 0