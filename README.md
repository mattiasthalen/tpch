# TPC-H

## DAG / Lineage
```mermaid
flowchart LR
    subgraph tpch.bronze["tpch.bronze"]
        direction LR
        snp__tpch__customers(["snp__tpch__customers"])
        snp__tpch__line_items(["snp__tpch__line_items"])
        snp__tpch__nations(["snp__tpch__nations"])
        snp__tpch__orders(["snp__tpch__orders"])
        snp__tpch__part_suppliers(["snp__tpch__part_suppliers"])
        snp__tpch__parts(["snp__tpch__parts"])
        snp__tpch__regions(["snp__tpch__regions"])
        snp__tpch__suppliers(["snp__tpch__suppliers"])
    end

    subgraph tpch.silver["tpch.silver"]
        direction LR
        bag__tpch__customers(["bag__tpch__customers"])
        bag__tpch__nations(["bag__tpch__nations"])
        bag__tpch__order_line(["bag__tpch__order_line"])
        bag__tpch__orders(["bag__tpch__orders"])
        bag__tpch__part_suppliers(["bag__tpch__part_suppliers"])
        bag__tpch__parts(["bag__tpch__parts"])
        bag__tpch__regions(["bag__tpch__regions"])
        bag__tpch__suppliers(["bag__tpch__suppliers"])
    end

    subgraph tpch.main["tpch.main"]
        direction LR
        customer(["customer"])
        lineitem(["lineitem"])
        nation(["nation"])
        orders(["orders"])
        part(["part"])
        partsupp(["partsupp"])
        region(["region"])
        supplier(["supplier"])
    end

    %% tpch.bronze -> tpch.silver
    snp__tpch__customers --> bag__tpch__customers
    snp__tpch__line_items --> bag__tpch__order_line
    snp__tpch__nations --> bag__tpch__nations
    snp__tpch__orders --> bag__tpch__orders
    snp__tpch__part_suppliers --> bag__tpch__part_suppliers
    snp__tpch__parts --> bag__tpch__parts
    snp__tpch__regions --> bag__tpch__regions
    snp__tpch__suppliers --> bag__tpch__suppliers

    %% tpch.main -> tpch.bronze
    customer --> snp__tpch__customers
    lineitem --> snp__tpch__line_items
    nation --> snp__tpch__nations
    orders --> snp__tpch__orders
    part --> snp__tpch__parts
    partsupp --> snp__tpch__part_suppliers
    region --> snp__tpch__regions
    supplier --> snp__tpch__suppliers
```

## ERDs
### Bronze
```mermaid
flowchart LR
    customer(["customer"])
    lineitem(["lineitem"])
    nation(["nation"])
    orders(["orders"])
    part(["part"])
    partsupp(["partsupp"])
    region(["region"])
    supplier(["supplier"])

    lineitem --> orders
    lineitem --> part
    lineitem --> supplier
    orders --> customer
    customer --> nation
    nation --> region
    supplier --> nation
    partsupp --> part
    partsupp --> supplier
```

### Silver
```mermaid
flowchart LR
    %% Bags
    bag__tpch__customers("bag__tpch__customers")
    bag__tpch__nations("bag__tpch__nations")
    bag__tpch__order_lines("bag__tpch__order_lines")
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
    _hook__order_line(["_hook__order_line"])
    _hook__part_supplier(["_hook__part_supplier"])

    %% Relations
    _hook__order_line --> bag__tpch__order_lines

    bag__tpch__order_lines --> _hook__order
    _hook__order --> bag__tpch__orders
    bag__tpch__orders --> _hook__customer
    _hook__customer --> bag__tpch__customers
    bag__tpch__customers --> _hook__nation
    _hook__nation --> bag__tpch__nations
    bag__tpch__nations --> _hook__region
    _hook__region --> bag__tpch__regions

    bag__tpch__order_lines -->_hook__part
    _hook__part --> bag__tpch__parts

    bag__tpch__order_lines --> _hook__supplier
    _hook__supplier --> bag__tpch__suppliers
    bag__tpch__suppliers --> _hook__nation

    _hook__part_supplier --> bag__tpch__part_suppliers
    bag__tpch__part_suppliers --> _hook__supplier
    bag__tpch__part_suppliers --> _hook__part
```