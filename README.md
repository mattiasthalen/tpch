# TPC-H

## DAG / Lineage
```mermaid
flowchart LR
    subgraph tpch.bronze["tpch.bronze"]
        direction LR
        raw__tpch__customers(["raw__tpch__customers"])
        raw__tpch__line_items(["raw__tpch__line_items"])
        raw__tpch__nations(["raw__tpch__nations"])
        raw__tpch__orders(["raw__tpch__orders"])
        raw__tpch__part_suppliers(["raw__tpch__part_suppliers"])
        raw__tpch__parts(["raw__tpch__parts"])
        raw__tpch__regions(["raw__tpch__regions"])
        raw__tpch__suppliers(["raw__tpch__suppliers"])
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
        bag__tpch__line_items(["bag__tpch__line_items"])
        bag__tpch__nations(["bag__tpch__nations"])
        bag__tpch__orders(["bag__tpch__orders"])
        bag__tpch__part_suppliers(["bag__tpch__part_suppliers"])
        bag__tpch__parts(["bag__tpch__parts"])
        bag__tpch__regions(["bag__tpch__regions"])
        bag__tpch__suppliers(["bag__tpch__suppliers"])
        int__customers(["int__customers"])
        int__suppliers(["int__suppliers"])
        int__uss_bridge(["int__uss_bridge"])
        int__uss_bridge__customers(["int__uss_bridge__customers"])
        int__uss_bridge__line_items(["int__uss_bridge__line_items"])
        int__uss_bridge__orders(["int__uss_bridge__orders"])
        int__uss_bridge__part_suppliers(["int__uss_bridge__part_suppliers"])
        int__uss_bridge__parts(["int__uss_bridge__parts"])
        int__uss_bridge__suppliers(["int__uss_bridge__suppliers"])
    end

    subgraph tpch.gold["tpch.gold"]
        direction LR
        
        subgraph bridges
            _bridge__as_is(["_bridge__as_is"])
            _bridge__as_of(["_bridge__as_of"])
            _bridge__as_of_event(["_bridge__as_of_event"])
        end
        
        calendar(["calendar"])
        customers(["customers"])
        line_items(["line_items"])
        orders(["orders"])
        part_suppliers(["part_suppliers"])
        parts(["parts"])
        suppliers(["suppliers"])
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
    bag__tpch__customers --> int__customers
    bag__tpch__line_items --> int__uss_bridge__line_items
    bag__tpch__nations --> int__customers
    bag__tpch__nations --> int__suppliers
    bag__tpch__orders --> int__uss_bridge__line_items
    bag__tpch__orders --> int__uss_bridge__orders
    bag__tpch__part_suppliers --> int__uss_bridge__part_suppliers
    bag__tpch__parts --> int__uss_bridge__parts
    bag__tpch__regions --> int__customers
    bag__tpch__regions --> int__suppliers
    bag__tpch__suppliers --> int__suppliers
    int__customers --> int__uss_bridge__customers
    int__suppliers --> int__uss_bridge__suppliers
    int__uss_bridge__customers --> int__uss_bridge
    int__uss_bridge__customers --> int__uss_bridge__orders
    int__uss_bridge__line_items --> int__uss_bridge
    int__uss_bridge__orders --> int__uss_bridge
    int__uss_bridge__orders --> int__uss_bridge__line_items
    int__uss_bridge__part_suppliers --> int__uss_bridge
    int__uss_bridge__parts --> int__uss_bridge
    int__uss_bridge__parts --> int__uss_bridge__line_items
    int__uss_bridge__parts --> int__uss_bridge__part_suppliers
    int__uss_bridge__suppliers --> int__uss_bridge
    int__uss_bridge__suppliers --> int__uss_bridge__line_items
    int__uss_bridge__suppliers --> int__uss_bridge__part_suppliers

    %% tpch.silver -> tpch.gold
    bag__tpch__line_items --> line_items
    bag__tpch__orders --> orders
    bag__tpch__part_suppliers --> part_suppliers
    bag__tpch__parts --> parts
    int__customers --> customers
    int__suppliers --> suppliers
    int__uss_bridge --> _bridge__as_of
    int__uss_bridge --> calendar

    %% tpch.gold -> tpch.gold
    _bridge__as_of --> _bridge__as_is
    _bridge__as_of --> _bridge__as_of_event
    
    linkStyle default stroke:#666,stroke-width:2px

    %% Bronze shades
    classDef bronze_classic fill:#CD7F32,color:white
    classDef bronze_dark fill:#B87333,color:white
    classDef bronze_light fill:#E09756,color:white
    classDef bronze_antique fill:#966B47,color:white
    
    %% Silver shades
    classDef silver_classic fill:#C0C0C0,color:black
    classDef silver_dark fill:#A8A8A8,color:black
    classDef silver_light fill:#D8D8D8,color:black
    classDef silver_antique fill:#B4B4B4,color:black
    
    %% Gold shades
    classDef gold_classic fill:#FFD700,color:black
    classDef gold_dark fill:#DAA520,color:black
    classDef gold_light fill:#FFE55C,color:black
    classDef gold_antique fill:#CFB53B,color:black

    class tpch.bronze bronze_classic
    class raw__tpch__customers bronze_dark
    class raw__tpch__line_items bronze_dark
    class raw__tpch__nations bronze_dark
    class raw__tpch__orders bronze_dark
    class raw__tpch__part_suppliers bronze_dark
    class raw__tpch__parts bronze_dark
    class raw__tpch__regions bronze_dark
    class raw__tpch__suppliers bronze_dark
    
    class snp__tpch__customers bronze_light
    class snp__tpch__line_items bronze_light
    class snp__tpch__nations bronze_light
    class snp__tpch__orders bronze_light
    class snp__tpch__part_suppliers bronze_light
    class snp__tpch__parts bronze_light
    class snp__tpch__regions bronze_light
    class snp__tpch__suppliers bronze_light
    
    class tpch.silver silver_classic
    class bag__tpch__customers silver_dark
    class bag__tpch__line_items silver_dark
    class bag__tpch__nations silver_dark
    class bag__tpch__orders silver_dark
    class bag__tpch__part_suppliers silver_dark
    class bag__tpch__parts silver_dark
    class bag__tpch__regions silver_dark
    class bag__tpch__suppliers silver_dark
    
    class int__customers silver_antique
    class int__suppliers silver_antique

    class int__uss_bridge silver_light
    class int__uss_bridge__customers silver_light
    class int__uss_bridge__line_items silver_light
    class int__uss_bridge__nations silver_light
    class int__uss_bridge__orders silver_light
    class int__uss_bridge__part_suppliers silver_light
    class int__uss_bridge__parts silver_light
    class int__uss_bridge__regions silver_light
    class int__uss_bridge__suppliers silver_light
    
    class tpch.gold gold_classic
    class bridges gold_light
    class _bridge__as_is gold_dark
    class _bridge__as_of gold_dark
    class _bridge__as_of_event gold_dark
    
    class calendar gold_light
    class customers gold_light
    class line_items gold_light
    class nations gold_light
    class orders gold_light
    class part_suppliers gold_light
    class parts gold_light
    class regions gold_light
    class suppliers gold_light
```

## ERDs - Oriented Data Models
### Bronze
```mermaid
flowchart LR
    raw__tpch__customers(["raw__tpch__customers"])
    raw__tpch__line_items(["raw__tpch__line_items"])
    raw__tpch__nations(["raw__tpch__nations"])
    raw__tpch__orders(["raw__tpch__orders"])
    raw__tpch__part_suppliers(["raw__tpch__part_suppliers"])
    raw__tpch__regions(["raw__tpch__regions"])
    raw__tpch__suppliers(["raw__tpch__suppliers"])
    raw__tpch__parts(["raw__tpch__parts"])

    raw__tpch__line_items --> raw__tpch__orders
    raw__tpch__line_items --> raw__tpch__part_suppliers
    raw__tpch__orders --> raw__tpch__customers
    raw__tpch__customers --> raw__tpch__nations
    raw__tpch__nations --> raw__tpch__regions
    raw__tpch__suppliers --> raw__tpch__nations
    raw__tpch__part_suppliers --> raw__tpch__suppliers
    raw__tpch__part_suppliers --> raw__tpch__parts

    %% Bronze shades
    classDef bronze_classic fill:#CD7F32,color:white
    classDef bronze_dark fill:#B87333,color:white
    classDef bronze_light fill:#E09756,color:white
    classDef bronze_antique fill:#966B47,color:white

    class raw__tpch__customers bronze_dark
    class raw__tpch__line_items bronze_dark
    class raw__tpch__nations bronze_dark
    class raw__tpch__orders bronze_dark
    class raw__tpch__part_suppliers bronze_dark
    class raw__tpch__parts bronze_dark
    class raw__tpch__regions bronze_dark
    class raw__tpch__suppliers bronze_dark
```

### Silver
```mermaid
flowchart LR
    %% Relations
    bag__tpch__regions("bag__tpch__regions") --> _hook__region(["_hook__region"])
    bag__tpch__nations("bag__tpch__nations") --> _hook__region(["_hook__region"])
    bag__tpch__nations("bag__tpch__nations") --> _hook__nation(["_hook__nation"])
    bag__tpch__part_suppliers("bag__tpch__part_suppliers") --> _hook__part_supplier(["_hook__part_supplier"])
    bag__tpch__line_items("bag__tpch__line_items") ----> _hook__order(["_hook__order"])
    bag__tpch__line_items("bag__tpch__line_items") --> _hook__part_supplier(["_hook__part_supplier"])
    bag__tpch__part_suppliers("bag__tpch__part_suppliers") ----> _hook__supplier(["_hook__supplier"])
    bag__tpch__customers("bag__tpch__customers") ---> _hook__nation(["_hook__nation"])
    bag__tpch__customers("bag__tpch__customers") --> _hook__customer(["_hook__customer"])
    bag__tpch__orders("bag__tpch__orders") --> _hook__customer(["_hook__customer"])
    bag__tpch__orders("bag__tpch__orders") --> _hook__order(["_hook__order"])
    bag__tpch__suppliers("bag__tpch__suppliers") ----> _hook__nation(["_hook__nation"])
    bag__tpch__suppliers("bag__tpch__suppliers") --> _hook__supplier(["_hook__supplier"])
    bag__tpch__part_suppliers("bag__tpch__part_suppliers") --> _hook__part(["_hook__part"])
    bag__tpch__parts("bag__tpch__parts") --> _hook__part(["_hook__part"])

    %% Silver shades
    classDef silver_classic fill:#C0C0C0,color:black
    classDef silver_dark fill:#A8A8A8,color:black
    classDef silver_light fill:#D8D8D8,color:black
    classDef silver_antique fill:#B4B4B4,color:black

    class bag__tpch__customers silver_dark
    class bag__tpch__line_items silver_dark
    class bag__tpch__nations silver_dark
    class bag__tpch__orders silver_dark
    class bag__tpch__part_suppliers silver_dark
    class bag__tpch__parts silver_dark
    class bag__tpch__regions silver_dark
    class bag__tpch__suppliers silver_dark

    class _hook__customer silver_light
    class _hook__nation silver_light
    class _hook__order silver_light
    class _hook__part silver_light
    class _hook__part_supplier silver_light
    class _hook__region silver_light
    class _hook__supplier silver_light
```

### Gold
```mermaid
flowchart LR
    subgraph bridges
        direction LR
        _bridge__as_of(["_bridge__as_of"])
        _bridge__as_is(["_bridge__as_is"])
        _bridge__as_of_event(["_bridge__as_of_event"])
    end

    customers(["customers"])
    orders(["orders"])
    line_items(["line_items"])
    part_suppliers(["part_suppliers"])
    parts(["parts"])
    suppliers(["suppliers"])
    
    bridges --> customers
    bridges --> orders
    bridges --> line_items
    bridges --> part_suppliers
    bridges --> parts
    bridges --> suppliers

    %% Gold shades
    classDef gold_classic fill:#FFD700,color:black
    classDef gold_dark fill:#DAA520,color:black
    classDef gold_light fill:#FFE55C,color:black
    classDef gold_antique fill:#CFB53B,color:black

    class bridges gold_light
    class _bridge__as_is gold_dark
    class _bridge__as_of gold_dark
    class _bridge__as_of_event gold_dark
    
    class calendar gold_light
    class customers gold_light
    class line_items gold_light
    class nations gold_light
    class orders gold_light
    class part_suppliers gold_light
    class parts gold_light
    class regions gold_light
    class suppliers gold_light
```
