"""
main del parser
"""
from run import run_parser # ejecuta el parser llamando a parse_document que es la funcion principal de core.py

resultado = run_parser("data/raw_pages/index.json")

print(f"el resultado de el parseo de los html es:\n {resultado}")
