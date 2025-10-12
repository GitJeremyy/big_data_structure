from fastapi import APIRouter, Query
from services.schema_client import Schema
from services.statistics import Statistics
from services.relationships import get_profile
from services.sizing import Sizer

router = APIRouter()

amazon_sample_schema = """{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "description": "",
  "type": "object",
  "properties": {
    "OrderLine": {
      "type": "object",
      "properties": {
        "date": {
          "type": "string",
          "format": "date",
          "minLength": 1
        },
        "quantity": {
          "type": "number"
        },
        "deliveryDate": {
          "type": "string",
          "format": "date",
          "minLength": 1
        },
        "comment": {
          "type": "string",
          "minLength": 1
        },
        "grade": {
          "type": "number"
        },
        "Product": {
          "type": "object",
          "properties": {
            "IDP": {
              "type": "number"
            },
            "name": {
              "type": "string",
              "minLength": 1
            },
            "price": {
              "type": "object",
              "properties": {
                "amount": {
                  "type": "number"
                },
                "currency": {
                  "type": "string",
                  "minLength": 1
                },
                "vat_rate": {
                  "type": "number"
                }
              },
              "required": [
                "amount",
                "currency",
                "vat_rate"
              ]
            },
            "brand": {
              "type": "string",
              "minLength": 1
            },
            "description": {
              "type": "string",
              "minLength": 1
            },
            "image_url": {
              "type": "string",
              "minLength": 1
            },
            "categories": {
              "type": "array",
              "items": {
                "required": [],
                "properties": {}
              }
            },
            "supplier": {
              "type": "object",
              "properties": {
                "IDS": {
                  "type": "number"
                },
                "name": {
                  "type": "string",
                  "minLength": 1
                },
                "SIRET": {
                  "type": "string",
                  "minLength": 1
                },
                "headOffice": {
                  "type": "string",
                  "minLength": 1
                },
                "revenue": {
                  "type": "number"
                }
              },
              "required": [
                "IDS",
                "name",
                "SIRET",
                "headOffice",
                "revenue"
              ]
            }
          },
          "required": [
            "IDP",
            "name",
            "price",
            "brand",
            "description",
            "image_url",
            "categories",
            "supplier"
          ]
        }
      },
      "required": [
        "date",
        "quantity",
        "deliveryDate",
        "comment",
        "grade",
        "Product"
      ]
    },
    "Stock": {
      "type": "object",
      "properties": {
        "quantity": {
          "type": "number"
        },
        "location": {
          "type": "string",
          "minLength": 1
        }
      },
      "required": [
        "quantity",
        "location"
      ]
    },
    "WareHouse": {
      "type": "object",
      "properties": {
        "IDW": {
          "type": "number"
        },
        "location": {
          "type": "string",
          "minLength": 1
        },
        "capacity": {
          "type": "number"
        }
      },
      "required": [
        "IDW",
        "location",
        "capacity"
      ]
    },
    "Client": {
      "type": "object",
      "properties": {
        "IDC": {
          "type": "number"
        },
        "ln": {
          "type": "string",
          "minLength": 1
        },
        "fn": {
          "type": "string",
          "minLength": 1
        },
        "address": {
          "type": "string",
          "minLength": 1
        },
        "nationality": {
          "type": "string",
          "minLength": 1
        },
        "birthdate": {
          "type": "string",
          "minLength": 1
        },
        "email": {
          "type": "string",
          "minLength": 1
        }
      },
      "required": [
        "IDC",
        "ln",
        "fn",
        "address",
        "nationality",
        "birthdate",
        "email"
      ]
    }
  },
  "required": [
    "OrderLine",
    "Stock",
    "WareHouse",
    "Client"
  ]
}
"""

@router.get("/bytesCalculator")
async def calculate_bytes(db_signature: str = Query("DB4", description="DB signature")):
    # Build core objects
    schema = Schema(amazon_sample_schema)
    stats = Statistics()
    profile = get_profile(db_signature)  # relationship & collections profile

    # Optional: console debug
    stats.describe()
    schema.print_entities_and_relations()

    # Relationship-aware sizing
    sizer = Sizer(schema, profile, stats)
    sizes = sizer.compute_collection_sizes()

    return {
        "message": "Byte calculation successful!",
        "db_signature": db_signature,
        "database_total": sizes["Database_Total"]["total_human"],
        "collections": sizes
    }