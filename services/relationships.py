# services/relationships.py

from dataclasses import dataclass
from typing import List, Optional, Dict

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


# ---------- DB1 profile ----------
# Signature (from slides): Prod{[Cat],Supp}, St, Wa, OL, Cl
# Meaning:
# - Product embeds MANY Categories (avg ≈ 2)
# - Product embeds ONE Supplier
# - Stock is a collection with FKs to Product(IDP) and WareHouse(IDW)
# - OrderLine is a collection with FKs to Product(IDP) and Client(IDC)
DB1 = DenormProfile(
    name="DB1",
    collections=["Product", "Stock", "WareHouse", "OrderLine", "Client"],
    relationships=[
        RelationshipSpec("Product", "Categories", "embed_many", avg_multiplicity=2.0),
        RelationshipSpec("Product", "Supplier",   "embed_one"),

        RelationshipSpec("Stock", "Product",   "fk", fk_fields=["IDP"]),
        RelationshipSpec("Stock", "WareHouse", "fk", fk_fields=["IDW"]),

        RelationshipSpec("OrderLine", "Product", "fk", fk_fields=["IDP"]),
        RelationshipSpec("OrderLine", "Client",  "fk", fk_fields=["IDC"]),
    ],
)

DB4 = DenormProfile(
    name="DB4",
    collections=["Stock", "WareHouse", "OrderLine", "Client"],
    relationships=[
        # Inside OrderLine
        RelationshipSpec("OrderLine", "Product", "embed_one"),
        RelationshipSpec("OrderLine", "Client",  "fk", fk_fields=["IDC"]),

        # Inside Product (which is embedded under OrderLine)
        RelationshipSpec("Product", "Categories", "embed_many", avg_multiplicity=2.0),
        RelationshipSpec("Product", "Supplier",   "embed_one"),

        # Stock remains a separate collection with FKs
        RelationshipSpec("Stock", "Product",   "fk", fk_fields=["IDP"]),
        RelationshipSpec("Stock", "WareHouse", "fk", fk_fields=["IDW"]),
    ],
)

# More profiles (DB2, DB3, ...) can be defined here following the same pattern
PROFILES: Dict[str, DenormProfile] = {
    "DB1": DB1,
    # "DB2": DB2, ...
    # "DB3": DB3, ...
    "DB4": DB4,
    # "DB5": DB5, ...
}

def get_profile(signature: str) -> DenormProfile:
    try:
        return PROFILES[signature]
    except KeyError:
        raise ValueError(f"Unknown DB signature '{signature}'. Available: {', '.join(PROFILES)}")