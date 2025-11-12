class Statistics:
    """
    Holds dataset-level and entity-level statistics 
    used for calculating collection sizes, sharding, etc.
    """

    # ============================================================
    # APPROXIMATE BYTE SIZES (MongoDB-style, single source of truth)
    # ============================================================
    SIZE_NUMBER = 8          # numeric field (int, float)
    SIZE_INTEGER = 8
    SIZE_STRING = 80         # short string (e.g. names)
    SIZE_DATE = 20           # ISO-style date string
    SIZE_LONGSTRING = 200    # long text (e.g. description)
    SIZE_ARRAY = 0           # arrays contribute only via their contents
    SIZE_UNKNOWN = 8         # fallback for unknown types
    SIZE_KEY = 12            # key overhead (key + colon + type header)

    @classmethod
    def size_map(cls):
        """Mapping from logical type to byte size."""
        return {
            "number": cls.SIZE_NUMBER,
            "integer": cls.SIZE_INTEGER,
            "string": cls.SIZE_STRING,
            "date": cls.SIZE_DATE,
            "longstring": cls.SIZE_LONGSTRING,
            "array": cls.SIZE_ARRAY,
            "unknown": cls.SIZE_UNKNOWN,
            "key": cls.SIZE_KEY,
        }

    # ============================================================
    # DATASET STATISTICS
    # ============================================================
    def __init__(self):
        # Core dataset volumes
        self.nb_clients = 10**7           # 10 million customers
        self.nb_products = 10**5          # 100,000 products
        self.nb_orderlines = 4 * 10**9    # 4 billion order lines
        self.nb_warehouses = 200          # 200 warehouses

        # Behavioural averages
        self.avg_orders_per_customer = 100
        self.avg_products_per_customer = 20
        self.avg_categories_per_product = 2
        self.nb_distinct_brands = 5000
        self.nb_apple_products = 50
        self.nb_days = 365

        # Infrastructure / context
        self.nb_servers = 1000

    # ============================================================
    # COLLECTION HELPERS
    # ============================================================
    def get_collection_count(self, name: str) -> int:
        """Return the number of documents expected for a given collection."""
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
            # One stock row per product per warehouse
            return self.nb_products * self.nb_warehouses
        # Default fallback
        return 0

    # ============================================================
    # DEBUG / INSPECTION
    # ============================================================
    def describe(self):
        """Print a summary of the dataset statistics."""
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
    
    # ============================================================
    # SHARDING STATISTICS
    # ============================================================

    def compute_sharding_stats(self):
        """
        Compute average document and key distribution across servers
        for all six sharding strategies from the exercise.
        Returns a dict but also prints a readable summary.
        """
        results = {}
        nb_servers = self.nb_servers

        combos = [
            ("Stock", "#IDP", self.nb_products),
            ("Stock", "#IDW", self.nb_warehouses),
            ("OrderLine", "#IDC", self.nb_clients),
            ("OrderLine", "#IDP", self.nb_products),
            ("Product", "#IDP", self.nb_products),
            ("Product", "#brand", self.nb_distinct_brands),
        ]

        print("\n=== SHARDING DISTRIBUTION STATISTICS ===")
        header = f"{'Collection':<12} {'Shard key':<8} {'Docs/server':>15} {'Keys/server':>15} {'Active servers':>17} {'Docs/active':>15}"
        print(header)
        print("-" * len(header))

        for coll, key, nb_keys_total in combos:
            nb_docs_total = self.get_collection_count(coll)
            nb_active_servers = min(nb_keys_total, nb_servers)

            # Average distributions
            docs_per_server = nb_docs_total / nb_servers
            docs_per_active_server = nb_docs_total / nb_active_servers
            keys_per_server = max(1, nb_keys_total / nb_active_servers)

            key_name = f"{coll}_{key}"

            results[key_name] = {
                "collection": coll,
                "shard_key": key,
                "nb_docs_total": nb_docs_total,
                "nb_keys_total": nb_keys_total,
                "nb_servers_total": nb_servers,
                "nb_active_servers": nb_active_servers,
                "docs_per_server": docs_per_server,
                "docs_per_active_server": docs_per_active_server,
                "keys_per_server": keys_per_server,
            }

            print(f"{coll:<12} {key:<8} {docs_per_server:15,.0f} {keys_per_server:15,.2f} {nb_active_servers:17,} {docs_per_active_server:15,.0f}")

        print("-" * len(header))
        print(f"Total servers available: {nb_servers:,}")

        return results