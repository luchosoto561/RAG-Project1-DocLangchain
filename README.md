El proyecto consiste en un sistema RAG que recupera informacion de la documentacion oficial de Langchain, para contestarte, siendo un experto en Langchain. Tiene
una primer etapa de Indexing, donde se cargan los datos en la base de datos vectorial y una segunda etapa que es retrieve donde, el usuario realiza una query, y el
RAG recupera info. de la base de datos vectorial para responder a la query con mayor precision teniendo en cuenta la documentacion oficial.

Arbol de diseno:

.vscode
/data -> contiene los html descargados y los json que representan el parseo de esos html.

/docs -> contiene decisiones de diseno y actualizaciones (donde cuando termino de trabajar un dia anoto donde me quede y que es lo que hice).

/indexing:

- crawler -> descarga los html de la documentacion oficial
- parser -> convierte cada HTML crudo en una representacion estructurada y limpia del contenido (json estructurado con jerarquia H1/H2/H3, bloques basicos y
  url_final para poder chunkear y citar despues). Es la base sobre la que luego podremos chunkear. Conserva jerarquia, anchors y bloques tecnicos.
- chunker -> chunkea todos los .json que se encuentran en data/parsed_pages, respetando un limite de "tokens" (consideramos que un token tiene 4 palabras)

README.md
