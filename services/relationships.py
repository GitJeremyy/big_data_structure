from dataclasses import dataclass
from typing import List, Optional, Dict
from services.statistics import Statistics

@dataclass(frozen=True)
class RelationshipSpec:
    from_entity: str
    to_entity: str
    stored_as: str           # "fk" | "embed_one" | "embed_many"
    fk_fields: Optional[List[str]] = None
    avg_multiplicity: Optional[float] = None  # only for embed_many


@dataclass(frozen=True)
class DenormProfile:
    name: str
    # Top-level collections that physically exist in this database design
    collections: List[str]
    # How relationships are stored for this design
    relationships: List[RelationshipSpec]

# ---------- DB0 profile ----------
# Signature : Prod, Cat, Supp, St, Wa, OL, Cl
# Meaning:
# - All entities are separate collections
DB0 = DenormProfile(
    name="DB0",
    collections=["Product", "Category", "Supplier", "Stock", "Warehouse", "OrderLine", "Client"],
    relationships=[
        RelationshipSpec("Stock", "Product", "fk", fk_fields=["IDP"]),
        RelationshipSpec("Stock", "Warehouse", "fk", fk_fields=["IDW"]),

        RelationshipSpec("OrderLine", "Product", "fk", fk_fields=["IDP"]),
        RelationshipSpec("OrderLine", "Client", "fk", fk_fields=["IDC"]),

        RelationshipSpec("Product", "Categories", "fk"),
        RelationshipSpec("Product", "Supplier", "fk", fk_fields=["IDS"])
    ],
)

# ---------- DB1 profile ----------
# Signature : Prod{[Cat],Supp}, St, Wa, OL, Cl
# Meaning:
# - Product embeds MANY Categories (avg ≈ 2)
# - Product embeds ONE Supplier
# - Stock is a collection with FKs to Product(IDP) and WareHouse(IDW)
# - OrderLine is a collection with FKs to Product(IDP) and Client(IDC)
DB1 = DenormProfile(
    name="DB1",
    collections=["Product", "Stock", "Warehouse", "OrderLine", "Client"],
    relationships=[
        RelationshipSpec("Product", "Categories", "embed_many", avg_multiplicity=Statistics().avg_categories_per_product),
        RelationshipSpec("Product", "Supplier",   "embed_one"),

        RelationshipSpec("Stock", "Product",   "fk", fk_fields=["IDP"]),
        RelationshipSpec("Stock", "Warehouse", "fk", fk_fields=["IDW"]),

        RelationshipSpec("OrderLine", "Product", "fk", fk_fields=["IDP"]),
        RelationshipSpec("OrderLine", "Client",  "fk", fk_fields=["IDC"]),
    ],
)

# ---------- DB2 profile ----------
# Signature : Prod{[Cat],Supp,[St]}, Wa, OL, Cl
# Meaning:
# - Product embeds MANY Categories (avg ≈ 2)
# - Product embeds ONE Supplier
# - Product embeds MANY Stock entries (200 warehouses so avg ≈ 200)
# - Stock is NOT a separate collection anymore
# - OrderLine is a collection with FKs to Product(IDP) and Client(IDC)
DB2 = DenormProfile(
    name="DB2",
    collections=["Product", "Warehouse", "OrderLine", "Client"],
    relationships=[
        RelationshipSpec("Product", "Categories", "embed_many", avg_multiplicity=Statistics().avg_categories_per_product),
        RelationshipSpec("Product", "Supplier",   "embed_one"),
        RelationshipSpec("Product", "Stock",      "embed_many", avg_multiplicity=Statistics().nb_warehouses),

        RelationshipSpec("Stock", "Warehouse", "fk", fk_fields=["IDW"]),

        RelationshipSpec("OrderLine", "Product",  "fk", fk_fields=["IDP"]),
        RelationshipSpec("OrderLine", "Client",   "fk", fk_fields=["IDC"]),
    ],
)

# ---------- DB3 profile ----------
# Signature : St{Prod{[Cat],Supp}}, Wa, OL, Cl
# Meaning:
# - Stock embeds ONE Product
# - Product embeds MANY Categories (avg ≈ 2)
# - Product embeds ONE Supplier
# - Stock is a collection with FK to WareHouse(IDW)
# - OrderLine is a collection with FKs to Product(IDP) and Client(IDC)
DB3 = DenormProfile(
    name="DB3",
    collections=["Stock", "Warehouse", "OrderLine", "Client"],
    relationships=[
        RelationshipSpec("Stock", "Product", "embed_one"),
        RelationshipSpec("Stock", "Warehouse", "fk", fk_fields=["IDW"]),

        RelationshipSpec("Product", "Categories", "embed_many", avg_multiplicity=Statistics().avg_categories_per_product),
        RelationshipSpec("Product", "Supplier",   "embed_one"),

        RelationshipSpec("OrderLine", "Product",  "fk", fk_fields=["IDP"]),
        RelationshipSpec("OrderLine", "Client",   "fk", fk_fields=["IDC"]),
    ],
)

# ---------- DB4 profile ----------
# Signature : St, Wa, OL{Prod{[Cat],Supp}},Cl
# Meaning:
# - OrderLine embeds ONE Product
# - OrderLine has FK to Client(IDC)
# - Product embeds MANY Categories (avg ≈ 2)
# - Product embeds ONE Supplier
# - Stock is a collection with FKs to Product(IDP) and WareHouse(IDW)
DB4 = DenormProfile(
    name="DB4",
    collections=["Stock", "Warehouse", "OrderLine", "Client"],
    relationships=[
        RelationshipSpec("OrderLine", "Product", "embed_one"),
        RelationshipSpec("OrderLine", "Client",  "fk", fk_fields=["IDC"]),

        RelationshipSpec("Product", "Categories", "embed_many", avg_multiplicity=Statistics().avg_categories_per_product),
        RelationshipSpec("Product", "Supplier",   "embed_one"),

        RelationshipSpec("Stock", "Product",   "fk", fk_fields=["IDP"]),
        RelationshipSpec("Stock", "Warehouse", "fk", fk_fields=["IDW"]),
    ],
)

# ---------- DB5 profile ----------
# Signature : Prod{[Cat],Supp,[OL]}, St, Wa, Cl
# Meaning:
# - Product embeds MANY Categories (avg ≈ 2)
# - Product embeds ONE Supplier
# - Product embeds MANY OrderLines (avg ≈ 400)
# - Stock is a collection with FKs to Product(IDP) and WareHouse(IDW)
# - OrderLine is NOT a separate collection anymore
DB5 = DenormProfile(
    name="DB5",
    collections=["Product", "Stock", "Warehouse", "Client"],
    relationships=[
        RelationshipSpec("Product", "Categories", "embed_many", avg_multiplicity=Statistics().avg_categories_per_product),
        RelationshipSpec("Product", "Supplier",   "embed_one"),
        RelationshipSpec("Product", "OrderLines", "embed_many", avg_multiplicity=Statistics().nb_orderlines / Statistics().nb_products),

        RelationshipSpec("OrderLine", "Client",   "fk", fk_fields=["IDC"]),

        RelationshipSpec("Stock", "Product",   "fk", fk_fields=["IDP"]),
        RelationshipSpec("Stock", "Warehouse", "fk", fk_fields=["IDW"]),
    ],
)

PROFILES: Dict[str, DenormProfile] = {
    "DB0": DB0,
    "DB1": DB1,
    "DB2": DB2,
    "DB3": DB3,
    "DB4": DB4,
    "DB5": DB5,
}

def get_profile(signature: str) -> DenormProfile:
    try:
        return PROFILES[signature]
    except KeyError:
        raise ValueError(f"Unknown DB signature '{signature}'. Available: {', '.join(PROFILES)}")
