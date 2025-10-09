"""
main del parser
"""
from indexing.parser.run import run_parser # ejecuta el parser llamando a parse_document que es la funcion principal de core.py

resultado = run_parser()

print(f"el resultado de el parseo de los html es:\n {resultado}")
