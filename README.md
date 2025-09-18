# TwitterScraper

## Investigación tweets borrados vs censurados

- tweetid borrado: https://twitter.com/_/status/1857346454286671936
- tweetid censurado: https://twitter.com/vitoquiles/status/1858894725882786246

Posibles scrapers, se basan en que tengan fotos/vídeos:

1. https://typefully.com/tools/twitter-image-downloader

- tweet borrado: devuelve 404
- tweet censurado: devuelve 200 y el contenido

2. https://redketchup.io/twitter-downloader

- tweet borrado: devuelve vacío, pero podría ser la misma respuesta que si existiese y no tuviese multimedia
- tweet censurado: devuelve media en el array
