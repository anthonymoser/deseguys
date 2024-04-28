import msgspec
import json

import networkx as nx
import pandas as pd
import usaddress 
import probablepeople as pp 
from business_class import Entity
from messy_data import clean_street
from qng import GraphSchema 

import duckdb as d 
duck = d.connect('deseguys.duckdb', read_only = True)

### DATA FUNCTIONS ###
def get_entities(name = None, street = None, file_number = None, search_types: list = ['companies', 'contracts', 'campaigns'], manual_override = False):
    entity_types = {
        "companies": ["company"],
        "contracts": ["contract"], 
        "campaigns": ["committee", "donor"]
    }
    included_types = []
    for st in search_types:
        included_types.extend(entity_types[st])
    included_types = f"{str(included_types)[1:-1]}"
    
    print("included types", included_types)
    base_sql = f"""
                SELECT DISTINCT 
                    e.*, 
                    pri.name as link_to, 
                    pri.entity_type as primary_type
                FROM 
                    entities e 
                JOIN 
                    entities pri 
                ON 
                    e.index_type = pri.index_type
                    AND e.index_value = pri.index_value
                    AND pri.is_primary = 1
                WHERE 
                    pri.entity_type IN ({included_types})
                """

    results = []
    if name:
        sql = base_sql + f" AND e.name like '{name}' ORDER BY e.entity_type asc;"
        results.extend(duck.execute(sql).fetchall())
    
    if name and 'campaigns' in search_types:
        sql = f"""
                SELECT DISTINCT 
                    e.*, 
                    pri.name as link_to, 
                    pri.entity_type as primary_type
                FROM 
                    entities e 
                JOIN 
                    entities pri 
                ON 
                    e.index_type = pri.index_type
                    AND e.index_value = pri.index_value
                    AND pri.is_primary = 1
                WHERE 
                    pri.entity_type = 'committee'
                    AND pri.name like '{name}'
                    ORDER BY e.entity_type asc
        """
        results.extend(duck.execute(sql).fetchall())
        
    if street:
        sql = base_sql + f" AND e.street like '{street}' ORDER BY e.entity_type asc;"
        results.extend(duck.execute(sql).fetchall())
        
    if file_number: 
        try:
            file_number = int(file_number)
            file_number = f"{file_number:08}"
        except:
            pass 
        
        sql = base_sql + f" AND e.index_type in ('llc_file_number', 'corp_file_number') AND e.index_value like '{file_number}' ORDER BY e.entity_type asc;"
        results.extend(duck.execute(sql).fetchall())
        
    print(f"found {len(results)} entities")
    if len(results) > 5000 and manual_override is False:
        raise Exception(f"This search has produced {len(results)} results; the application may crash trying to render it. Do you want to cancel and narrow your search, or proceed anyway?")
    entities = {}
    for r in results:
        e = Entity(*r)
        entities[e.id] = e 
    return entities

def get_entities_by_index(index_type: str, index_value :str, is_primary = 0):
    results = duck.execute("""
        SELECT 
            e.*, 
            pri.name as link_to, 
            pri.entity_type as primary_type
        FROM 
            entities e 
        JOIN 
            entities pri 
        ON 
            e.index_type = pri.index_type
            AND e.index_value = pri.index_value
            AND pri.is_primary = 1
        WHERE 
            e.index_type = ?
            AND e.index_value = ?
            AND e.is_primary = ?
        """, 
        [index_type, index_value, is_primary]
    ).fetchall()
    entities = {}
    for r in results:
        e = Entity(*r)
        entities[e.id] = e
    return entities  

def get_entities_by_indexes(index_type: str, index_values :list, is_primary = 0):
    results = duck.sql(f"""
        SELECT 
            e.*, 
            pri.name as link_to, 
            pri.entity_type as primary_type
        FROM 
            entities e 
        JOIN 
            entities pri 
        ON 
            e.index_type = pri.index_type
            AND e.index_value = pri.index_value
            AND pri.is_primary = 1
        WHERE 
            e.index_type = '{index_type}'
            AND e.index_value in ({str(index_values)[1:-1]})
            AND e.is_primary = {is_primary}
        """
    ).fetchall() 

    entities = {}
    for r in results:
        e = Entity(*r)
        entities[e.id] = e
    return entities 


def get_linked_entities(entities:dict, already_imported:list = []):
    linked_entities = {}
    both_searches = []
    primary_searches = []
    
    for entity in entities:
        e = entities[entity]
        try:
            if e.primary_type in ['company', 'contract']:
                both_searches.append(f"{e.index_type}-{e.index_value}")
            else:
                primary_searches.append(f"{e.index_type}-{e.index_value}")
        except Exception as ex:
            print(ex)
            print(e)
    bs = pd.DataFrame(data=both_searches, columns=['idx']).drop_duplicates()
    ps = pd.DataFrame(data=primary_searches, columns=['idx']).drop_duplicates()
    aie = pd.DataFrame(data=already_imported, columns = ['already_imported'])
    
    both_results = duck.sql(""" 
        SELECT DISTINCT 
            e.*, 
            pri.name as link_to, 
            pri.entity_type as primary_type 
        FROM 
            entities e
        JOIN 
            entities pri 
        ON 
            e.index_type = pri.index_type
            AND e.index_value = pri.index_value
            AND pri.is_primary = 1
        WHERE
            e.id not in (SELECT already_imported FROM aie) 
            AND CONCAT(e.index_type, '-', e.index_value) IN (SELECT idx FROM bs)
    """).fetchall()
    
    primary_results = duck.sql(""" 
        SELECT DISTINCT 
            e.*, 
            pri.name as link_to, 
            pri.entity_type as primary_type 
        FROM 
            entities e
        JOIN 
            entities pri 
        ON 
            e.index_type = pri.index_type
            AND e.index_value = pri.index_value
            AND pri.is_primary = 1
        WHERE
            e.id not in (SELECT already_imported FROM aie) 
            AND CONCAT(e.index_type, '-', e.index_value) IN (SELECT idx FROM bs)
            AND e.is_primary = 1
    """).fetchall()
    
    linked_entities = {}
    for r in both_results:
        e = Entity(*r)
        linked_entities[e.id] = e 
    
    for r in primary_results:
        e = Entity(*r)
        linked_entities[e.id] = e 
        
    return linked_entities




def clean_columns(df:pd.DataFrame)->pd.DataFrame:
    lowercase = { 
        c: c.lower().strip().replace(' ', '_') 
        for c in df.columns }
    df = df.rename(columns=lowercase)
    return df

def load_schema(filename:str):
    with open(filename, 'r') as f:
        return msgspec.json.decode(f.read(), type=GraphSchema)

def get_metadata():
    return duck.sql("select data_source, data_link, max(sync_date) as sync_date from metadata group by data_source, data_link").fetchall()

### GRAPH FUNCTIONS ###

def graph_entities(G, graph_factory, entities:list, merged:list = []):
    # print(entities)
    G = nx.compose(G, graph_factory.make_graphs([ e.to_dict() for e in entities ], "il_sos"))
    excluded_nodes = get_excluded_nodes(G)
    for en in excluded_nodes:
        try:
            if " DISSOLUTION" in G.nodes[en]['label'] or "REVOKED " in G.nodes[en]['label']:
                for nbr in G.neighbors(en):
                    if G.nodes[nbr]['type'] == "company":
                        G.nodes[nbr]['type'] = "company (inactive)"
                    
            G.remove_node(en)    
        except Exception as e:
            print(e, en)
            continue
    G.remove_edges_from(nx.selfloop_edges(G)) 
    return G 


    

def get_excluded_nodes(G):
    return [ n for n in G.nodes() if has_excluded_name(n) ]
    

def has_excluded_name(node_label:str):
    excluded = ["INVOLUNTARY", "VACANT", "VACATED", "SOLE OFFICER", "None", "SAME ", "REVOKED ", " DISSOLUTION", "UNACCEPTABLE ", "MERGED ", "WITHDRAWN"]
    for e in excluded:
        if e in node_label or e.strip() == node_label.strip():
            return True 
    # if no excluded terms are in the node label, return false 
    return False 


def get_alias_ids(G, nodes:list):
    full_list = []
    for n in nodes:
        if has_excluded_name(n) is False:
            try:
                full_list += G.nodes[n]['alias_ids']
            except Exception as e:
                full_list.append(n)
    return full_list 


# def expand_nodes(G, nodes:list, search_types:list)->dict:
#     entities = {}
#     addresses = [] 
#     for n in nodes:
#         try:
#             match G.nodes(data="type")[n]:
#                 case "address":
#                     results = get_entities(street=n, search_types = search_types)
#                 case "company": 
#                     results = get_entities_by_index(G.nodes()[n]['index_type'], G.nodes()[n]['index_value']) 
#                 case "committee": 
#                     pass 
#                 case _: 
#                     results = get_entities(name=n, search_types = search_types)
#             entities = { **entities, **results}
#         except Exception as e:
#             print(e)
#             print(n)
#             print(G.nodes(n))
#             continue
    
#     return entities
def expand_nodes(G, nodes:list, search_types: list = ['companies', 'contracts', 'campaigns'], already_imported:list = [])->dict:
    entity_types = {
        "companies": ["company"],
        "contracts": ["contract"], 
        "campaigns": ["committee", "donor"]
    }
    included_types = []
    for st in search_types:
        included_types.extend(entity_types[st])
    included_types = f"{str(included_types)[1:-1]}"
    print(f"Expanding {included_types}")
    
    streets = [] 
    names = []
    companies = []
    for n in nodes:
        try:
            match G.nodes(data="type")[n]:
                case "address":
                    streets.append(n)
                case "company": 
                    companies.append(f"{G.nodes[n]['index_type']}-{G.nodes[n]['index_value']}") 
                case "committee": 
                    pass 
                case _: 
                    names.append(n) 
            
        except Exception as e:
            print(e)
            print(n)
            print(G.nodes(n))
            continue
    sf = pd.DataFrame(data = streets, columns=['street']).drop_duplicates()
    nf = pd.DataFrame(data = names, columns = ["name"]).drop_duplicates()
    cf = pd.DataFrame(data = companies, columns = ["idx"]).drop_duplicates()
    aie = pd.DataFrame(data = already_imported, columns = ["already_imported"])
    
    results = duck.sql(f""" 
        SELECT DISTINCT 
            e.*, 
            pri.name as link_to, 
            pri.entity_type as primary_type 
        FROM 
            entities e
        JOIN 
            entities pri 
        ON 
            e.index_type = pri.index_type
            AND e.index_value = pri.index_value
            AND pri.is_primary = 1
        WHERE
            pri.entity_type IN ({included_types})
            AND e.id not in (SELECT already_imported FROM aie) 
            AND (
                e.name in (SELECT name FROM nf) 
                OR e.street IN (SELECT street FROM sf)
                OR CONCAT(e.index_type, '-', e.index_value) IN (SELECT idx FROM cf)
            )
    """).fetchall()
    print(f"{len(results)} entities found")
    entities = {}
    for r in results:
        e = Entity(*r)
        entities[e.id] = e 
    return entities
     
def expand_graph(G:nx.MultiGraph, node_list = [], search_types:list = [], already_imported = []) ->dict:
    # if nothing is selected, everything is selected
    if len(node_list) == 0:
        node_list = list(G.nodes())
        
    print(f"expanding {len(node_list)} nodes")
    nodes = list(set(get_alias_ids(G, node_list)))
    entities = expand_nodes(G, nodes, search_types, already_imported)        
    return entities


def extract_name_parts(G:nx.MultiGraph):
    name_nodes = get_nodes_by_attribute(G, "tidy", "name")
    names_parts = {}
    for n in name_nodes:
        try:
            name = G.nodes[n]['label'].replace('.', '').strip().upper()
            parts = pp.parse(name)
            names_parts[n] = parts
        except Exception as e:
            print(e, n)
            continue
    
    name_records = []
    company_name_records = []
    for name in names_parts:
        parts = names_parts[name]
        if "CorporationName" in dict(names_parts[name]).values():
            parts = [p[0].replace(',', '').replace('.', '').upper() for p in parts]
            company_name_records.append({"node_id": name, "company_name": " ".join(parts)})
        else:
            record = {part[1]: part[0].replace(',', '').replace('.', '') for part in parts}
            record["node_id"] = name
            name_records.append(record)  
            
    return pd.DataFrame(name_records).fillna(''), pd.DataFrame(company_name_records).fillna('')


def clean_streets(G: nx.MultiGraph):
    street_nodes = get_nodes_by_attribute(G, "tidy", "address")
    for sn in street_nodes:
        raw = G.nodes[sn].get("label", sn)
        label = clean_street(raw)
        G.nodes[sn]["label"] = label
    return G 


def extract_street_parts(G:nx.MultiGraph):
    street_nodes = get_nodes_by_attribute(G, "tidy", "address")
    street_nodes = [ street for street in street_nodes if 'PO BOX' not in street and 'P.O. BOX' not in street ]
    records = []
    for street in street_nodes :
        try:         
            tags = usaddress.tag(street.upper())
            records.append({"node_id": street, **tags[0]})
        except Exception as e:
            print(G.nodes[street])
            continue
    return pd.DataFrame(records).fillna('')


def get_ilsos_node(G, nodes:list):
    for n in nodes:
        if n in G:
            if G.nodes[n]['data_source'] == "il_sos":
                return n 


def combine_nodes(G, nodes:list):
    nodes = sorted(nodes)
    ilsos_node = get_ilsos_node(G, nodes)
    keep_node = nodes[0] if ilsos_node is None else ilsos_node
    merge_data = {}
    final_type = G.nodes[keep_node]['type']
    priority = {
        "company": 0, 
        "committee": 1,
        "name": 2, 
        "address": 3,
        "donor": 4, 
        "contract": 5 
    }
    for n in nodes:
        if n in G.nodes and n != keep_node:
            if priority.get(G.nodes[n]['type'], 5) < priority[final_type]:
                final_type = G.nodes[n]['type']
                
            merge_data[n] = G.nodes[n]
            G = nx.identified_nodes(G, keep_node, n)
            
    if keep_node in G:
        G.nodes[keep_node]['alias_ids'] = nodes
        md = G.nodes[keep_node]['merge_data'] if "merge_data" in G.nodes[keep_node].keys() else {}
        G.nodes[keep_node]['merge_data'] = { **md, **merge_data}
        G.nodes[keep_node]['type'] = final_type
    return G 


def get_node_names(G)->dict:
    node_names = {} 
    for n in G.nodes:
        name = G.nodes[n].get("label", n)
        node_names[name] = n
    return node_names


def tidy_up(G):
    G = clean_streets(G)
    nf, cnf = extract_name_parts(G)
    sr = extract_street_parts(G)
    
    nd = get_probable_duplicates(nf, ['GivenName', 'Surname', 'SuffixGenerational']) 
    cnd = get_probable_duplicates(cnf, ['company_name'])
    sd = get_probable_duplicates(sr, ['AddressNumber', 'StreetName', 'OccupancyIdentifier'])
    duplicates = nd + cnd + sd 
    
    for d in duplicates:
        G = combine_nodes(G, d)
    return G    


def tidy_up_companies(G):
    nf, cnf = extract_name_parts(G)
    cnd = get_probable_duplicates(cnf, ['company_name'])
    for d in cnd:
        G = combine_nodes(G, d)
    return G    


def parse_amounts(G):
    amount_fields = {
        "contract": "award_amount", 
        "donation": "amount"
    }
    graph = G.copy()
    for u, v in graph.edges():
        links = graph[u][v]
        for link in links:
            details_string = graph[u][v][link].get('details', '{}')
            details = json.loads(details_string)
            amount_field = amount_fields.get(graph[u][v][link]['type'], "no_amount")
            link_size = int(float(details.get(amount_field, 1)))
            graph[u][v][link]['size'] = link_size
            graph.nodes[v]['size'] = graph.nodes[v].get('size', 1) + link_size
            graph.nodes[u]['size'] = graph.nodes[u].get('size', 1) + link_size
    return graph


def get_probable_duplicates(df, grouping):
    if len(df) == 0:
        return []
    else:
        grouping = [g for g in grouping if g in df.columns]
        probable_duplicates = (
            df
            .reset_index()
            .groupby(grouping)
            .agg({"node_id": ";".join, "index":"count"})
            .pipe(lambda df: df[df['index'] > 1])
        )
        return [pd.split(';') for pd in list(probable_duplicates.node_id)]



def get_nodes_by_attribute(G: nx.MultiGraph, key:str, filter_value:str) -> list:
    node_attributes = G.nodes(data=key, default = None)
    return [ n[0] for n in node_attributes if n[1] == filter_value ]


def get_colors(G):
    node_reserved = {
        "company": "black", 
        "address": "#f9cf13",
        "name": "#dd0f04",
    }
    edge_reserved = {
        "manager": "#e515ed",
        "agent": "#00c3dd",  
        "address": "#adadad",
        "company": "black", 
        "president": "#7a15ed", 
        "secretary": "#2937f4" 
    }
    colors = [
        '#1b9e77',
        '#d95f02',
        '#7570b3',
        '#e7298a',
        '#66a61e',
        '#e6ab02',
        '#a6761d',
        '#666666',
        '#666666',
        '#666666'
        ]
    
    node_types = [t for t in set(dict(G.nodes(data="type", default=None)).values()) if t is not None]
    edge_types = set()
    for (u, v, k, c) in G.edges(data='type', keys=True, default=None):
        if c is not None:
            edge_types.add(c)
    edge_types = list(edge_types)
    node_colors = get_colormap(node_types, colors, node_reserved)
    edge_colors = get_colormap(edge_types, colors, edge_reserved)
    return node_colors, edge_colors 


def get_colormap(types, colors, reserved):
    colormap = {}
    for count, t in enumerate(types):
        colormap[t] = reserved.get(t, colors[count])
    return colormap 


def deduplicate_edges(G):
    records = [ {"source": edge[0], "target": edge[1], **edge[2]} for edge in G.edges(data=True) ]
    df = pd.DataFrame(records).drop_duplicates()
    source = df.source
    target = df.target
    attr = df.drop(columns=["source", "target"]).to_dict('records')
    G.clear_edges()
    G.add_edges_from(zip(source, target, attr))
    return G    

    
def get_connected_nodes(G, node, nbrhood:dict = {}) -> dict:
    graph = G.to_undirected(as_view=True)
    if node in graph:
        nbrs = nx.neighbors(graph, node)
        nbrhood[node] = nbrs
        for n in nbrs:
            if n not in nbrhood:
                nbrhood.update(get_connected_nodes(graph, n, nbrhood))
        return nbrhood
    else:
        return nbrhood 



def get_node_label(G, node):
    return G.nodes[node]['label'] if 'label' in G.nodes[node].keys() else node 


def join_unique_list(x):
    return "; ".join(list(set(x.dropna())))


def get_overview_frame(entities):
    return( 
        pd.DataFrame([entities[e].to_dict() for e in entities])
            [['name', 'street', 'link_type', 'link_to', 'is_primary']]
            .drop_duplicates()
            .pipe(lambda df: df[df.is_primary == 0])
            .groupby(['name', 'street', 'link_type'])
            .agg({"link_to": join_unique_list})
            .reset_index()
            .sort_values(['name', 'street', 'link_type'])
            .pivot(index=['name', 'street'], columns = 'link_type', values='link_to').fillna('')
            .reset_index()
        )
    
### DATA PORTAL FUNCTIONS

def search_data_set(resource, keyword):
    results = client.get(resource, q=keyword)
    return results

def format_address_search(row:dict):
    parts = ['AddressNumber', 'StreetNamePreDirectional', 'StreetName', 'OccupancyType', 'OccupancyIdentifier']
    search_parts = []
    for p in parts:
        if p in row and row[p] != "":
            search_parts.append(row[p].replace('#', "").strip())
            
    search = " ".join(search_parts)
    return search 


def get_street_searches(streets:pd.DataFrame):
    return (streets
                .assign(search = lambda df: df.apply(lambda row: format_address_search(row), axis=1))
                .pipe(lambda df: df[df['search'] != ""])
                [['node_id', 'search']]
                .groupby('search')['node_id'].apply(list)
                .reset_index()
            ).to_dict('records')
    
    
def search_data_portal(keywords:list, resource_id:str = 'rsxa-ify5', prefix:str = "CONTRACT"):
    results = []
    for k in keywords:
        result = search_data_set(resource_id, k['search'])
        for row in result:
            row['node_id'] = k['node_id']
            row['result_id'] = f"{prefix}-{row['purchase_order_contract_number']}-{row['revision_number']}"
            results.append(row)
    if len(results) > 0:
        return pd.DataFrame(results).explode('node_id')
    else:
        return pd.DataFrame()




### Path graph    
def get_path_graph(G, node_1, node_2):
    path_nodes = set()
    shortest_paths = list(nx.all_shortest_paths(G.to_undirected(as_view=True), node_1, node_2))
    for path in shortest_paths:
        path_nodes.update(path)
    return nx.induced_subgraph(G, list(path_nodes))



def export_sheet(df, writer, sheet):
    col_widths = {
        "relationship": 10,
        "type": 10, 
        "data_source": 20,
        "node_id": 20, 
        "tidy": 10,
    }
    workbook=writer.book
    df.to_excel(writer, sheet, index=False)
    wrap = workbook.add_format({'text_wrap': True})
    for column in df:
        column_width = max(df[column].astype(str).map(len).max(), len(column))
        col_idx = df.columns.get_loc(column)
        writer.sheets[sheet].set_column(col_idx, col_idx, col_widths.get(column, 25), wrap)
            