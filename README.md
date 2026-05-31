# AV1 Recompress

AV1 Recompress egy asztali, Tkinter alapú batch videó-átkódoló alkalmazás AV1 munkafolyamatokhoz. A célja, hogy több videófájlt lehessen sorba rendezni, beállításokkal ellátni, majd SVT-AV1 vagy NVENC alapú kódolással feldolgozni úgy, hogy a hangsávok, feliratok, metaadatok és minőségellenőrzési lépések kezelése is egy helyen maradjon.

Az alkalmazás a helyi gépen fut, és a grafikus felület mellett opcionális HTTP felületet is tud indítani. A projektben külön modulok kezelik a fájlbetöltést, a várólistát, a kódoló worker folyamatokat, az adatbázis-alapú állapotmentést, a többnyelvű feliratokat és az önellenőrző/autotest indítási módokat.

A futtatáshoz Python 3 szükséges, valamint a `requirements.txt` fájlban szereplő Python csomagok. A tényleges átkódoláshoz a használt munkafolyamattól függően külső parancssori eszközök is kellenek, például `ffmpeg`, `ffprobe`, `mkvmerge`, SVT-AV1 vagy NVENC kompatibilis encoder. Fejlesztői indításhoz:

```powershell
pip install -r requirements.txt
python -m av1_recompress.app
```

Ez a repó csak az alkalmazás forrását és a futtatáshoz szükséges alapvető projektfájlokat tartalmazza. Build kimenetek, lokális logok, adatbázisok, cache fájlok, csomagolt `.exe`/`.pyz` fájlok és mintavideók szándékosan nincsenek verziókövetésben.
