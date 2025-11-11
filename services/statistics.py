class Statistics:
    """
    Holds dataset-level and entity-level statistics 
    used for calculating collection sizes, sharding, etc.
    """

    # official approximate byte sizes (class-level, single source of truth)
    # integers/floats
    SIZE_NUMBER = 8
    SIZE_INTEGER = 8
    # short string
    SIZE_STRING = 80
    # ISO-style date string
    SIZE_DATE = 20
    # long text (description, etc.)
    SIZE_LONGSTRING = 200
    # arrays contribute only via their items; no base overhead
    SIZE_ARRAY = 0
    # small nested key-value
    SIZE_OBJECT = 12
    # embedded or referenced object
    SIZE_REFERENCE = 12
    # fallback for unknown types
    SIZE_UNKNOWN = 8

    @classmethod
    def size_map(cls):
        """Convenience mapping for consumers that look up by logical type name."""
        return {
            "number": cls.SIZE_NUMBER,
            "integer": cls.SIZE_INTEGER,
            "string": cls.SIZE_STRING,
            "date": cls.SIZE_DATE,
            "longstring": cls.SIZE_LONGSTRING,
            "array": cls.SIZE_ARRAY,
            "object": cls.SIZE_OBJECT,
            "reference": cls.SIZE_REFERENCE,
            "unknown": cls.SIZE_UNKNOWN,
        }

    def __init__(self):
        # Core dataset stats (from the homework)
        self.nb_clients = 10**7            # 10 million customers
        self.nb_products = 10**5           # 100,000 products
        self.nb_orderlines = 4 * 10**9     # 4 billion order lines
        self.nb_warehouses = 200           # 200 warehouses

        # Derived behavioural stats
        self.avg_orders_per_customer = 100
        self.avg_products_per_customer = 20
        self.avg_categories_per_product = 2
        self.nb_distinct_brands = 5000
        self.nb_apple_products = 50
        self.nb_days = 365

        # Infrastructure stats
        self.nb_servers = 1000

    def get_collection_count(self, name: str) -> int:
        n = name.lower()
        if n == "client":
            return self.nb_clients
        if n == "product":
            return self.nb_products
        if n == "orderline":
            return self.nb_orderlines
        if n == "warehouse":
            return self.nb_warehouses
        if n == "stock":
            # One stock row per product (even if quantity = 0)
            return self.nb_products * self.nb_warehouses
        # Default fallback
        return 0

    def describe(self):
        """Print a summary of the statistics."""
        print("\n=== DATASET STATISTICS ===")
        print(f"Clients: {self.nb_clients:,}")
        print(f"Products: {self.nb_products:,}")
        print(f"OrderLines: {self.nb_orderlines:,}")
        print(f"Warehouses: {self.nb_warehouses:,}")
        print(f"Avg orders/client: {self.avg_orders_per_customer}")
        print(f"Avg products/client: {self.avg_products_per_customer}")
        print(f"Avg categories/product: {self.avg_categories_per_product}")
        print(f"Distinct brands: {self.nb_distinct_brands}")
        print(f"Apple products: {self.nb_apple_products}")
        print(f"Orders distributed over {self.nb_days} days")
        print(f"Servers: {self.nb_servers}")