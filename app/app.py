
import io 
import networkx as nx
from ipysigma import Sigma
from util import *
import pandas as pd 
import asyncio
import json 
from shiny import App, Inputs, Outputs, Session, reactive, render, ui
from shinywidgets import output_widget, render_widget
from shiny.types import FileInfo
from htmltools import TagList, div
from qng import GraphSchema, NodeFactory, LinkFactory, GraphFactory, SigmaFactory, Element, QNG

graph_schema = load_schema('graph_schemas/entity.qngs')
graph_factory = GraphFactory(
    node_factories= list(dict(graph_schema.node_factories).values()), 
    link_factories= graph_schema.link_factories
)

def help_link(id:str):
     return ui.input_action_link(id, ui.HTML('<i class="fa fa-question-circle" aria-hidden="true"></i>') )

question_circle =  ui.HTML('<i class="fa fa-question-circle" aria-hidden="true"></i>')

help_text = {
    "subgraph": ui.TagList(
                    ui.tags.p("Click a node on the graph or select some from the dropdown. The subgraph is anything connected to your selection."),
                    ui.tags.p("Preview to confirm it's what you want. Then you can delete it, or keep it and delete everything else.")
                ),
    "simple_paths": "Choose a starting point and and ending point and click 'Show' to see only nodes and edges that connect them. Click 'Clear' to return to the full graph.",
    "select": ui.TagList(ui.tags.p("Select node(s) by clicking on the graph and/or choosing from the dropdown."), ui.tags.p("Once selected, you can merge them together, remove them, or use them to search for more connections.")),
    "and_directly_connected": "in addition to what you selected, include any nodes directly linked to those nodes.",
    "auto_merge_likely_duplicates": 'automatically merge nodes that are probably the same person/address - ("LASTNAME, FIRSTNAME JR" and "FIRSTNAME LASTNAME JR")',
    "name": "the name of a person or company. Use '%' as a wildcard", 
    "street": "the street address - exclude city/state/zip",
    "file_number": "corporate/llc file number",
    "save/load_qng_graph_file": "Save a copy of this graph data in the Quick Network Graph (QNG) format, upload a graph you saved earlier, or upload one from Quick Network Graph at bit.ly/qng. Uploads are added to the current graph"
}

def tooltip(title:str):
    return ui.tooltip(
        ui.span(title, question_circle),
        help_text[title.lower().replace(" ", "_")],
        placement="right"
    )
    
def data_freshness():
    rows = get_metadata()
    return ui.TagList([ ui.HTML(f"<p>Data Source: {r[0]} <br/><a href = {r[1]} target='_'> {r[1]} </a>  <br/>Sync date: {r[2].strftime('%Y-%m-%d')}</p>") for r in rows ])
### SHINY APP ###

app_ui = ui.page_fillable(
    ui.head_content(ui.include_css("font-awesome-4.7.0 2/css/font-awesome.min.css")),
    
    ui.tags.style("""
        .card-header {background-color: #efefef !important;}
        .fa-question-circle {margin: 0 0 0 5px; color:darkgrey;}
        .card li {margin-top: 5px !important;}
        .graph-control-button { width:100%; margin: 0px 0px 0px 0px;}
        .modal-content {width: 75%;}                     
        .modal-content li {margin-top 10px !important;}
        .modal-body .btn {margin: 5px 5px 5px 5px; align=center;}
        .modal-body #modal_buttons {margin: 10px 10px 10px 10px;}
        .modal-header {background-color: #ececec;}
        # .btn btn-default action-button {min-width:120px;}
        #card_tooltip {align-text:left;}
        
        """
    ),
    
    ui.div(
        ui.h2("Dese Guys", style="{margin: 0 0 0 0;}"),
        ui.TagList(ui.help_text("A "), ui.a("Public Data Tools", href="http://publicdatatools.com"), ui.help_text(" project for making "), ui.a("Quick Network Graphs", href="http://bit.ly/qng"), ui.help_text(" using business data from the "), ui.a("IL SOS", href="https://www.ilsos.gov/data/bus_serv_home.html")),
    ),
    
    ui.div(
       
        ui.accordion(
            ui.accordion_panel("Controls",
                ui.layout_columns(
                    ui.navset_card_tab(
                        ui.nav_panel("Search",
                            ui.layout_columns(
                                ui.card(
                                    ui.layout_column_wrap(
                                        ui.input_text("name_search",tooltip("Name"), "Michael Tadin%"),
                                        ui.input_text("addr_search",tooltip("Street")),
                                        ui.input_text("file_number_search", tooltip("File number")),
                                    ),
                                ), 
                                ui.card(
                                    ui.input_checkbox("search_companies", "companies", value=True),
                                    ui.input_checkbox("search_contracts", "contracts", value=False),
                                    ui.input_checkbox("search_campaigns", "campaigns", value=False), 
                                ),
                                ui.input_task_button("search_btn", "Search"),
                                col_widths = {
                                    "lg": (9,3,12), 
                                    "md": (7,5,12)
                                }    
                            ),
                        ),
                        
                        ui.nav_panel("Save/Load",
                            ui.layout_columns(
                                ui.card(
                                    ui.card_header("Export"),
                                    ui.tooltip(
                                        ui.download_button("export_graph", "Export HTML"),
                                        "Save a standalone HTML file of the interactive graph",
                                        placement="right"
                                    ),
                                    ui.download_button("export_entities", "Export source data"),
                                    ui.download_button("export_xlsx", "Export table view"),
                                ),
                                ui.card(
                                    ui.card_header(tooltip("Save/Load QNG Graph File")),
                                    ui.input_file("file1", "", accept=[".qng"], multiple=True, placeholder='*.qng', width="100%"),
                                    ui.card_footer(
                                        ui.download_button("save_graph_data", "Save QNG Graph File"),
                                    ),
                                ),
                                col_widths=(4,8),
                                heights_equal = False,
                                fill=False
                            ),
                        ),
                        ui.nav_panel("Filter", 
                        ui.layout_columns(
                                ui.card(
                                    ui.card_header(tooltip("Subgraph")),
                                    ui.layout_column_wrap(
                                                ui.input_action_button("preview_subgraph", "Preview", class_="graph-control-button"),
                                                ui.input_action_button("keep_subgraph", "Keep", class_="graph-control-button"),
                                                ui.input_action_button("remove_subgraph", "Remove", class_="graph-control-button"),
                                                ui.input_action_button("cancel_subgraph", "Cancel", class_="graph-control-button"),    
                                    width=(1/2),
                                    fill=False,
                                    ),
                                ),
                                ui.card(
                                    ui.card_header(tooltip("Simple paths")), 
                                    ui.layout_columns(
                                        ui.input_select("path_start", "Start", choices = []),
                                        ui.input_select("path_end", "End", choices = []),
                                    col_widths=(6,6)
                                    ),
                                    ui.card_footer(
                                        # ui.row(
                                            ui.input_action_button("clear_paths", "Clear"),
                                            ui.input_action_button("show_paths", "Show"),
                                        # )
                                    )
                    
                                ),
                                ui.card(                                    
                                    ui.input_checkbox("edge_size_amounts", "Set link size to match dollars for contracts and donations", value=False)
                                ),
                            col_widths=(4,6,2),
                            ),
                        ),
                        ui.nav_panel("Source Data", 
                            data_freshness(),
                        ), 
                        ui.nav_control(
                            ui.a(
                                "About This Tool",
                                href="https://docs.google.com/document/d/e/2PACX-1vQ-f_-G8qpi1VFIaMwdlPjkYVHcGm5qJFwy9hu9_K0cUY1KzFa7PN_xkbiiZa54Rdl6RsK3GBLbrpDg/pub",
                                target="_blank",
                            )
                        )
                        
                    ),
                    
                    
                    ui.card(
                        ui.card_header(tooltip("Select")),
                        ui.input_selectize("selected_nodes", "", choices=[], multiple=True, width="100%"),  
                        ui.input_checkbox("and_neighbors", tooltip("and directly connected"), value=False),
                        ui.input_checkbox("tidy", tooltip("auto merge likely duplicates"), value=False),
                        ui.card_footer(
                            ui.row(
                                ui.layout_columns(
                                    ui.input_action_button("combine", "Merge"),
                                    ui.input_action_button("remove", "Remove"),
                                    col_widths = (6,6)                                    
                                ),
                            ),
                            ui.tooltip(
                                ui.input_task_button("expand_btn", "What else connects?", width="100%"),
                                "Search for more names and addresses connected to the ones on the graph",
                                placement="bottom"
                            ),
                        ),
                    ),
                    col_widths = {
                        "lg": (9,3),
                        "md": (7, 5), 
                        "sm": (7, 5)
                    },
                ),
            ),
            ui.accordion_panel("Table view", 
                ui.output_data_frame("graph_summary")
            ),    
            
            
            id="accordion_controls"
        ),
        # id="mainbox",
    id="frustrating"
    ),
    output_widget("sigma_graph"),
    height="auto",
    title="Dese Guys"
)

def server(input, output, session):
    ### System State         
    entities = reactive.value([])
    all_entities = reactive.value({})  
    G = reactive.value(nx.MultiDiGraph())
    nodes = reactive.value({})
    build_count = reactive.value(0)
    merged = reactive.value([])
    connected_nodes = reactive.value({})
    search_types = reactive.value([])
    
    ### Factories
    SF = reactive.value(SigmaFactory(clickable_edges=True, edge_size='size'))
    viz = reactive.value()
    
    @render.data_frame
    def graph_nodes():
        if len(G()) > 0:
            return render.DataGrid(get_node_frame(G()), width="100%")
   
    @render.data_frame
    def graph_edges():
        if len(G()) > 0:
            return render.DataGrid(get_edge_frame(G()), width="100%")
    
    @render.data_frame
    def graph_summary():
        if len(G()) > 0: 
            return render.DataGrid(get_overview_frame(all_entities()), width="100%")
    
    def get_connected_to_selected():
        selected = get_selected_nodes()
        connected = {}
        for s in selected:
            connected.update(get_connected_nodes(G(), s, connected))
        # connected_nodes.set(connected)
        return connected 
    
    ### Clicked Search
    @reactive.effect
    def _():
        selected_search_types = []
        if input.search_companies() is True:
            selected_search_types.append("companies")
        if input.search_contracts() is True:
            selected_search_types.append("contracts")
        if input.search_campaigns() is True: 
            selected_search_types.append("campaigns")
        search_types.set(selected_search_types)
        
    @reactive.effect
    @reactive.event(input.search_btn)
    def search_click():
        search(input.name_search().upper(), input.addr_search().upper(), input.file_number_search(), search_types())
        
    @reactive.effect
    @reactive.event(input.manual_override)
    def search_override():
        ui.modal_remove()
        search(input.name_search().upper(), input.addr_search().upper(), input.file_number_search(), search_types(), manual_override=True)
        

    @ui.bind_task_button(button_id = 'search_btn')
    @reactive.extended_task
    async def search(search_name:str|None = None, search_street:str|None = None, file_number:str|None = None, search_types:list = [], manual_override = False) -> dict:
        print("async def search")
        try:
            entities = get_entities(name=search_name, street=search_street, file_number = file_number, search_types=search_types, manual_override=manual_override)
            linked_entities = get_linked_entities(entities)
            entities = { **entities, **linked_entities }
            return entities 
        except Exception as e:
            print(e)
            m = get_modal(
                title=None,
                prompt=str(e),
                buttons = [ui.modal_button("Cancel"), ui.input_action_button("manual_override", "Proceed")]
            )
            ui.modal_show(m)


    @reactive.effect
    @reactive.event(search.result)
    def set_entities():
        print("setting entities")
        result = search.result()
        if len(result) > 0:
            new_entities = [result[r] for r in result if r not in all_entities()]
            # print(new_entities)
            ae = { **all_entities(), **result }
            all_entities.set(ae)    
            entities.set(new_entities)
        else: 
            m = get_modal(
                title=None,
                prompt="No results",
                buttons = [ui.modal_button("OK")]
            )
            ui.modal_show(m)
        
        
    # graph option dropdowns
    # @reactive.Effect 
    # @reactive.event(G)
    # def _():
    #      edge_keys = [ None, *get_edge_keys(G())]
    #      node_keys = get_node_keys(G())
    #      ui.update_select(id = "edge_size_attribute", choices=edge_keys, selected = None)
    #      ui.update_select(id = "node_color_attribute", choices=node_keys, selected = "type")    
         
        
    ### Build graph from entities
    @reactive.effect
    @reactive.event(entities, search.result)
    def build_graph():
        print("building graph")
        graph = graph_entities(G().copy(), graph_factory, entities(), merged())
        
        if len(graph) > 0:
            G.set(graph)
            builds = build_count() + 1
            build_count.set(builds)
        
                      
    @reactive.Effect
    @reactive.event(input.file1)
    def _():
        f: list[FileInfo] = input.file1()
        datapath = f[0]['datapath']
        if f[0]['type'] == "application/octet-stream":
            with open(datapath, 'r') as f:
                graph_data = msgspec.json.decode(f.read(), type=QNG)
                mg = graph_data.multigraph()
                for node in mg.nodes():
                    try:
                        mg.nodes[node]['label'] = mg.nodes[node]['label'].upper()
                    except KeyError:
                        mg.nodes[node]['label'] = node.upper()
                    
                print(len(mg))
                if len(G()) > 0:
                    G.set(nx.compose(G(), mg))
                else:
                    G.set(mg)

                build_count.set(build_count() + 1)

    def get_modal(title:str|None = None, prompt:str|ui.TagList|None = None, buttons:list = [], size = "m", easy_close=False):
        ui.modal_remove()
        return ui.modal(
            prompt, 
            title=title,
            size=size,
            footer=ui.TagList([b for b in buttons]) if len(buttons) > 0 else None,
            easy_close = False if len(buttons) > 0 else True
        )
        


    ### ADVANCED CONTROLS
    
    @reactive.effect
    @reactive.event(input.preview_subgraph)
    def _():
        if len(G()) > 0:
            connected = get_connected_to_selected()
            connected_nodes.set(connected)
            
            if len(connected) > 0:  
                selected_SF = SigmaFactory(
                    layout_settings = {"StrongGravityMode": False}, 
                    node_color_palette = None, 
                    node_color = lambda n: "selected" if n in connected else "not selected"
                )
                layout = viz().get_layout()
                camera_state = viz().get_camera_state()
                viz.set(selected_SF.make_sigma(G(), node_colors="Dark2", layout=layout, camera_state=camera_state))
            else:
                m = get_modal(
                    title="You didn't select anything",
                    prompt="Select a node by clicking it, or choose some from the selection dropdown. Then you can preview a subgraph of everything able to connect to your selection, and either remove it all, or keep it and remove everything else.",
                    buttons = [ui.modal_button("OK")]
                    )
                ui.modal_show(m)
                
    
    @reactive.effect
    @reactive.event(input.keep_subgraph)
    def _():
        connected = get_connected_to_selected()
        if len(connected) == 0 and len(connected_nodes()) > 0:
            connected = connected_nodes()
        graph = nx.induced_subgraph(G(), connected)
        G.set(graph)
        
    
    @reactive.effect
    @reactive.event(input.remove_subgraph)
    def _():
        connected = get_connected_to_selected()
        if len(connected) == 0 and len(connected_nodes()) > 0:
            connected = connected_nodes()
        graph = G().copy()
        graph.remove_nodes_from(connected)
        G.set(graph)

        
    @reactive.effect
    @reactive.event(input.cancel_subgraph, input.clear_paths, input.edge_size_amounts)
    def _():
        graph = G().copy()
        G.set(graph)
        
    
    # Show Simple Paths
    @reactive.Effect
    @reactive.event(input.show_paths)        
    def _():
        print("generating path graph")
        PG = path_graph = get_path_graph(G(), input.path_start(), input.path_end())
        viz.set(SF().make_sigma(PG))

            

    ### SELECTED NODES 
    def get_selected_nodes():
        try:
            selected = [ n for n in input.selected_nodes() ]
            if viz().get_selected_node() is not None:
                selected += [ viz().get_selected_node() ]
                                
            neighbors = []
            if input.and_neighbors():
                for s in selected:
                    neighbors += list(G().neighbors(s))
                neighbors = list(set(neighbors))
                selected += neighbors
                
            print("SELECTED NODES: ", selected)
            return selected  
        except Exception as e:
            return []
    
    # Update node list 
    def update_node_choices(graph):
        node_names = get_node_names(graph)
        nodes.set(node_names)
        choices = {node_names[n]: n for n in sorted(list(node_names.keys()))}
        # choices = sorted(list(nodes().keys()))
        ui.update_selectize("selected_nodes", choices= choices)
        ui.update_select("path_start", choices=choices)
        ui.update_select("path_end", choices=choices)   


    ### Remove selected nodes
    @reactive.effect
    @reactive.event(input.remove)
    def _():
        graph = G().copy()
        graph.remove_nodes_from(get_selected_nodes())
        G.set(graph)
    
    
    ### Combine selected nodes
    @reactive.effect
    @reactive.event(input.combine)
    def _():
        
        selected = get_selected_nodes()
        print("Combining nodes: ", selected)
        new_graph = combine_nodes(G(), selected)
        
        merged.set( merged() + [selected])    
        G.set(new_graph)
        
            
    ### Clicked Expand Graph
    @reactive.effect
    @reactive.event(input.expand_btn)
    def expand_click():
        selected = get_selected_nodes()
        if len(selected) == 0:
            m = get_modal(
                title = None,
                prompt = f"Search for connections to all {len(G())} nodes in the graph?",
                buttons = [ui.modal_button("Cancel"), ui.input_action_button("expand_all", "Confirm")]
            )
            ui.modal_show(m)
        else:
            expand(G(), selected, search_types())
    
    @reactive.effect
    @reactive.event(input.expand_all)
    def _():
        ui.modal_remove()
        expand(G(), [], search_types(), already_imported_ids= list(all_entities().keys()))        

    @ui.bind_task_button(button_id='expand_btn')
    @reactive.extended_task
    async def expand(graph: nx.MultiDiGraph, selected_nodes:list = [], search_types:list = [], already_imported_ids: list = []):
        entities = expand_graph(graph, selected_nodes, search_types, already_imported_ids)
        linked_entities = get_linked_entities(entities, [*already_imported_ids, *list(entities.keys())])
        entities = { **entities, **linked_entities }
        return entities
        
    @reactive.effect
    @reactive.event(expand.result) 
    def set_expanded_graph():        
        result = expand.result()
        new_entities = [result[r] for r in result if r not in all_entities()]
        all_entities.set( {**all_entities(), **result })
        entities.set(new_entities)

    @reactive.effect
    def _():
        update_node_choices(G())
    


        
    # Tidy up
    @reactive.effect
    @reactive.event(G, build_count, input.tidy)
    def _():
        print(input.tidy(), len(G()))

        if input.tidy() is True and len(G()) > 0:
            graph = tidy_up(G())
        elif len(G()) > 0: 
            graph = tidy_up_companies(G())
        else: 
            graph = G()
        G.set(graph)
        

    # Make graph widget
    @reactive.effect
    @reactive.event(G) 
    def _():
        node_colors, edge_colors = get_colors(G())
        try:
            layout = viz().get_layout()
            camera_state = viz().get_camera_state()
            if input.edge_size_amounts() is True:
                size_graph = parse_amounts(G())
                size_sf = SigmaFactory(
                    layout_settings = {"StrongGravityMode": False, "gravity": .5, "outboundAttractionDistribution": False}, 
                    edge_size = 'size',
                    node_size = 'size',
                    clickable_edges=True,
                )
                viz.set(size_sf.make_sigma(size_graph, node_colors, edge_colors, layout=layout, camera_state=camera_state))
            else:
                viz.set(SF().make_sigma(G(), node_colors, edge_colors, layout = layout, camera_state = camera_state))
        except Exception as e:
            print(e)
            viz.set(SF().make_sigma(G(), node_colors, edge_colors))
       
        
    # Update visualization
    @render_widget(height="1000px")
    def sigma_graph():
        return viz()
    
    @render.download(filename="graph_export.html")
    def export_graph():
        with io.BytesIO() as bytes_buf:
            with io.TextIOWrapper(bytes_buf) as text_buf:
                Sigma.write_html(
                    G(),
                    path=text_buf,  
                    height=1000,
                    layout_settings=SF().layout_settings, 
                    edge_color = 'type', 
                    node_size = G().degree, 
                    node_size_range = (3, 30),
                    clickable_edges = False,
                    node_color = 'type',
                    node_color_palette = {
                        "company": "black", 
                        "address": "#f9cf13",
                        "person": "#dd0f04", 
                    },
                    edge_color_palette={
                        "manager": "#e515ed",
                        "agent": "#00c3dd",  
                        "address": "#adadad",
                        "company": "black", 
                        "president": "#7a15ed", 
                        "secretary": "#2937f4"
                    },
                    show_all_labels=True if len(G()) < 100 else False,
                    start_layout= len(G()) / 10
                )
                yield bytes_buf.getvalue()

    @render.download(filename="entities_export.csv" )
    def export_entities():        
        with io.BytesIO() as buf:
            df = (
                pd.DataFrame([ all_entities()[e].export_dict() for e in all_entities() ])
                .pipe(lambda df: df[df.entity_type != "donor"])
                .sort_values(['index_type', 'index_value', 'entity_type'])
                [['id', 'entity_type', 'name','street','city','state','postal_code','details','index_type','index_value','is_primary','link_type', 'link_to']]
            ).drop_duplicates().to_csv(buf, index=False)
            yield buf.getvalue()
            

    @render.download(filename="quick_network_graph.qng")
    def save_graph_data():
        # no_data_source = get_nodes_by_attribute(G(), "data_source", None)
        # for node in no_data_source:
        #     G().nodes[node]['data_source'] = "il_sos"
            
        adj = nx.to_dict_of_dicts(G())
        attrs = { n: G().nodes[n] for n in G().nodes()}
        qng = QNG(adjacency=adj, node_attrs=attrs, sigma_factory=SF())
        yield msgspec.json.encode(qng)


    @render.download(filename="overview.csv" )
    def export_xlsx():        
        with io.BytesIO() as buf:
            get_overview_frame(all_entities()).to_csv(buf)
            
            yield buf.getvalue()

app = App(app_ui, server)