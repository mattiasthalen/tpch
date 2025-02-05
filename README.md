# TPC-H

## DAG / Lineage
```mermaid
flowchart LR
    subgraph tpch.bronze["tpch.bronze"]
        direction LR
        raw__tpch__customers(["raw__tpch__customers"]):::bronze_2
        raw__tpch__line_items(["raw__tpch__line_items"]):::bronze_2
        raw__tpch__nations(["raw__tpch__nations"]):::bronze_2
        raw__tpch__orders(["raw__tpch__orders"]):::bronze_2
        raw__tpch__part_suppliers(["raw__tpch__part_suppliers"]):::bronze_2
        raw__tpch__parts(["raw__tpch__parts"]):::bronze_2
        raw__tpch__regions(["raw__tpch__regions"]):::bronze_2
        raw__tpch__suppliers(["raw__tpch__suppliers"]):::bronze_2

        snp__tpch__customers(["snp__tpch__customers"]):::bronze_1
        snp__tpch__line_items(["snp__tpch__line_items"]):::bronze_1
        snp__tpch__nations(["snp__tpch__nations"]):::bronze_1
        snp__tpch__orders(["snp__tpch__orders"]):::bronze_1
        snp__tpch__part_suppliers(["snp__tpch__part_suppliers"]):::bronze_1
        snp__tpch__parts(["snp__tpch__parts"]):::bronze_1
        snp__tpch__regions(["snp__tpch__regions"]):::bronze_1
        snp__tpch__suppliers(["snp__tpch__suppliers"]):::bronze_1
    end

    subgraph tpch.silver["tpch.silver"]
        direction LR
        bag__tpch__customers(["bag__tpch__customers"]):::silver_4
        bag__tpch__line_items(["bag__tpch__line_items"]):::silver_4
        bag__tpch__nations(["bag__tpch__nations"]):::silver_4
        bag__tpch__orders(["bag__tpch__orders"]):::silver_4
        bag__tpch__part_suppliers(["bag__tpch__part_suppliers"]):::silver_4
        bag__tpch__parts(["bag__tpch__parts"]):::silver_4
        bag__tpch__regions(["bag__tpch__regions"]):::silver_4
        bag__tpch__suppliers(["bag__tpch__suppliers"]):::silver_4
        int__uss_bridge(["int__uss_bridge"]):::silver_3
        int__uss_bridge__customers(["int__uss_bridge__customers"]):::silver_2
        int__uss_bridge__line_items(["int__uss_bridge__line_items"]):::silver_2
        int__uss_bridge__nations(["int__uss_bridge__nations"]):::silver_2
        int__uss_bridge__orders(["int__uss_bridge__orders"]):::silver_2
        int__uss_bridge__part_suppliers(["int__uss_bridge__part_suppliers"]):::silver_2
        int__uss_bridge__parts(["int__uss_bridge__parts"]):::silver_2
        int__uss_bridge__regions(["int__uss_bridge__regions"]):::silver_2
        int__uss_bridge__suppliers(["int__uss_bridge__suppliers"]):::silver_2
    end

    subgraph tpch.gold["tpch.gold"]
        direction LR
        _bridge__as_is(["_bridge__as_is"]):::gold_3
        _bridge__as_of(["_bridge__as_of"]):::gold_3
        calendar(["calendar"]):::gold_2
        customers(["customers"]):::gold_1
        line_items(["line_items"]):::gold_1
        nations(["nations"]):::gold_1
        orders(["orders"]):::gold_1
        part_suppliers(["part_suppliers"]):::gold_1
        parts(["parts"]):::gold_1
        regions(["regions"]):::gold_1
        suppliers(["suppliers"]):::gold_1
    end

    %% tpch.bronze -> tpch.bronze
    raw__tpch__customers --> snp__tpch__customers
    raw__tpch__line_items --> snp__tpch__line_items
    raw__tpch__nations --> snp__tpch__nations
    raw__tpch__orders --> snp__tpch__orders
    raw__tpch__part_suppliers --> snp__tpch__part_suppliers
    raw__tpch__parts --> snp__tpch__parts
    raw__tpch__regions --> snp__tpch__regions
    raw__tpch__suppliers --> snp__tpch__suppliers

    %% tpch.bronze -> tpch.silver
    snp__tpch__customers --> bag__tpch__customers
    snp__tpch__line_items --> bag__tpch__line_items
    snp__tpch__nations --> bag__tpch__nations
    snp__tpch__orders --> bag__tpch__orders
    snp__tpch__part_suppliers --> bag__tpch__part_suppliers
    snp__tpch__parts --> bag__tpch__parts
    snp__tpch__regions --> bag__tpch__regions
    snp__tpch__suppliers --> bag__tpch__suppliers

    %% tpch.silver -> tpch.silver
    bag__tpch__customers --> int__uss_bridge__customers
    bag__tpch__line_items --> int__uss_bridge__line_items
    bag__tpch__nations --> int__uss_bridge__nations
    bag__tpch__orders --> int__uss_bridge__line_items
    bag__tpch__orders --> int__uss_bridge__orders
    bag__tpch__part_suppliers --> int__uss_bridge__part_suppliers
    bag__tpch__parts --> int__uss_bridge__parts
    bag__tpch__regions --> int__uss_bridge__regions
    bag__tpch__suppliers --> int__uss_bridge__suppliers
    int__uss_bridge__customers --> int__uss_bridge
    int__uss_bridge__customers --> int__uss_bridge__orders
    int__uss_bridge__line_items --> int__uss_bridge
    int__uss_bridge__nations --> int__uss_bridge
    int__uss_bridge__nations --> int__uss_bridge__customers
    int__uss_bridge__nations --> int__uss_bridge__suppliers
    int__uss_bridge__orders --> int__uss_bridge
    int__uss_bridge__orders --> int__uss_bridge__line_items
    int__uss_bridge__part_suppliers --> int__uss_bridge
    int__uss_bridge__parts --> int__uss_bridge
    int__uss_bridge__parts --> int__uss_bridge__line_items
    int__uss_bridge__parts --> int__uss_bridge__part_suppliers
    int__uss_bridge__regions --> int__uss_bridge
    int__uss_bridge__regions --> int__uss_bridge__nations
    int__uss_bridge__suppliers --> int__uss_bridge
    int__uss_bridge__suppliers --> int__uss_bridge__line_items
    int__uss_bridge__suppliers --> int__uss_bridge__part_suppliers

    %% tpch.silver -> tpch.gold
    bag__tpch__customers --> customers
    bag__tpch__line_items --> line_items
    bag__tpch__nations --> nations
    bag__tpch__orders --> orders
    bag__tpch__part_suppliers --> part_suppliers
    bag__tpch__parts --> parts
    bag__tpch__regions --> regions
    bag__tpch__suppliers --> suppliers
    int__uss_bridge --> _bridge__as_is
    int__uss_bridge --> _bridge__as_of
    int__uss_bridge -.-> calendar

    inactive("Inactive")

    linkStyle default stroke:#666,stroke-width:2px
    classDef inactive stroke:#666,stroke-dasharray: 5 5,color:#999
    
    classDef bronze_0 fill:#cd7f32,color:white
    classDef bronze_1 fill:#dea16a,color:black
    classDef bronze_2 fill:#a65d21,color:white

    classDef silver_0 fill:#c0c0c0,color:black
    classDef silver_1 fill:#e8e8e8,color:black
    classDef silver_2 fill:#d4d4d4,color:black
    classDef silver_3 fill:#a8a8a8,color:black
    classDef silver_4 fill:#909090,color:white

    classDef gold_0 fill:#ffd700,color:black
    classDef gold_1 fill:#fff04d,color:black
    classDef gold_2 fill:#ffd700,color:black
    classDef gold_3 fill:#ccac00,color:black

    class tpch.bronze bronze_0
    class tpch.silver silver_0
    class tpch.gold gold_0
    class calendar inactive
    class inactive inactive
```

## ERDs
### Bronze
```mermaid
flowchart LR
    raw__tpch__customers(["raw__tpch__customers"])
    raw__tpch__line_items(["raw__tpch__line_items"])
    raw__tpch__nations(["raw__tpch__nations"])
    raw__tpch__orders(["raw__tpch__orders"])
    raw__tpch__part_suppliers(["raw__tpch__part_suppliers"])
    raw__tpch__parts(["raw__tpch__parts"])
    raw__tpch__regions(["raw__tpch__regions"])
    raw__tpch__suppliers(["raw__tpch__suppliers"])

    raw__tpch__line_items --> raw__tpch__orders
    raw__tpch__line_items --> raw__tpch__parts
    raw__tpch__line_items --> raw__tpch__suppliers
    raw__tpch__orders --> raw__tpch__customers
    raw__tpch__customers --> raw__tpch__nations
    raw__tpch__nations --> raw__tpch__regions
    raw__tpch__suppliers --> raw__tpch__nations
    raw__tpch__part_suppliers --> raw__tpch__parts
    raw__tpch__part_suppliers --> raw__tpch__suppliers
```

### Silver
```mermaid
flowchart LR
    %% Bags
    bag__tpch__customers("bag__tpch__customers")
    bag__tpch__nations("bag__tpch__nations")
    bag__tpch__line_items("bag__tpch__line_items")
    bag__tpch__orders("bag__tpch__orders")
    bag__tpch__part_suppliers("bag__tpch__part_suppliers")
    bag__tpch__parts("bag__tpch__parts")
    bag__tpch__regions("bag__tpch__regions")
    bag__tpch__suppliers("bag__tpch__suppliers")

    %% Hooks
    _hook__order(["_hook__order"])
    _hook__customer(["_hook__customer"])
    _hook__part(["_hook__part"])
    _hook__supplier(["_hook__supplier"])
    _hook__region(["_hook__region"])
    _hook__nation(["_hook__nation"])
    _hook__line_item(["_hook__line_item"])
    _hook__part_supplier(["_hook__part_supplier"])

    %% Relations
    _hook__line_item --> bag__tpch__line_items

    bag__tpch__line_items --> _hook__order
    _hook__order --> bag__tpch__orders
    bag__tpch__orders --> _hook__customer
    _hook__customer --> bag__tpch__customers
    bag__tpch__customers --> _hook__nation
    _hook__nation --> bag__tpch__nations
    bag__tpch__nations --> _hook__region
    _hook__region --> bag__tpch__regions

    bag__tpch__line_items -->_hook__part
    _hook__part --> bag__tpch__parts

    bag__tpch__line_items --> _hook__supplier
    _hook__supplier --> bag__tpch__suppliers
    bag__tpch__suppliers --> _hook__nation

    _hook__part_supplier --> bag__tpch__part_suppliers
    bag__tpch__part_suppliers --> _hook__supplier
    bag__tpch__part_suppliers --> _hook__part
```

### Gold
```mermaid
flowchart LR
    _bridge("_bridge")
    customers(["customers"])
    nations(["nations"])
    orders(["orders"])
    line_items(["line_items"])
    part_suppliers(["part_suppliers"])
    parts(["parts"])
    regions(["regions"])
    suppliers(["suppliers"])
    
    _bridge --> customers
    _bridge --> nations
    _bridge --> orders
    _bridge --> line_items
    _bridge --> part_suppliers
    _bridge --> parts
    _bridge --> regions
    _bridge --> suppliers
```