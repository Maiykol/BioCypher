def test_biocypher(core):
    assert core._dbms == 'neo4j'
    assert core._offline == True
    assert core._strict_mode == False


def test_log_missing_bl_types(core, translator):
    core._translator = translator
    core._translator.notype = {}
    assert core.log_missing_bl_types() == None

    core._translator.notype = {'a': 1, 'b': 2}
    mt = core.log_missing_bl_types()

    assert mt.get('a') == 1 and mt.get('b') == 2


# def test_access_translate(driver):

#     driver.start_ontology()

#     assert driver.translate_term('mirna') == 'MicroRNA'
#     assert (driver.reverse_translate_term('SideEffect') == 'sider')
#     assert (
#         driver.translate_query('MATCH (n:reactome) RETURN n') ==
#         'MATCH (n:Reactome.Pathway) RETURN n'
#     )
#     assert (
#         driver.reverse_translate_query(
#             'MATCH (n:Wikipathways.Pathway) RETURN n',
#         ) == 'MATCH (n:wikipathways) RETURN n'
#     )
