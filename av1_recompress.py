"""
================================================================================
AV1 BATCH ENCODER - TELJES MŰKÖDÉSI DOKUMENTÁCIÓ
================================================================================

ÁTTEKINTÉS
==========
Ez az alkalmazás batch videó átkódolást végez AV1 formátumba, támogatva:
- NVENC (NVIDIA GPU) és SVT-AV1 (CPU) kódolókat
- Párhuzamos feldolgozást (multi-worker architektúra)
- VMAF/PSNR minőség mérést
- Automatikus CQ/CRF optimalizálást
- Hangsáv manipulációt
- Adatbázis alapú állapot mentést

ARCHITEKTÚRA KOMPONENSEK
=========================

1. FŐBB SZÁLAK ÉS QUEUE-K
   -----------------------
   
   a) Főszál (Main Thread / GUI Thread):
      - Tkinter GUI futtatása
      - Felhasználói interakciók kezelése
      - GUI frissítések végrehajtása (check_encoding_queue)
      - Adatbázis műveletek koordinálása
      
   b) Encoding Coordinator Thread:
      - encoding_worker() függvény
      - Kódolandó videók kiválasztása
      - NVENC/SVT queue-k feltöltése
      - Eredmények gyűjtése
      - Automatikus CQ állítás VMAF alapján
      
   c) NVENC Worker Threads (1-3 db):
      - nvenc_worker(worker_index) függvény
      - NVENC queue feldolgozása
      - GPU-alapú kódolás (párhuzamos)
      - ab-av1 használata CQ meghatározáshoz
      - Validálás VirtualDub2-vel (frame count)
      
   d) SVT-AV1 Worker Thread (1 db):
      - svt_worker() függvény
      - SVT queue feldolgozása
      - CPU-alapú kódolás (egyedi)
      - Validálás VirtualDub2-vel
      - Fallback másolás ha kódolás sikertelen
      
   e) VMAF/PSNR Worker Thread (1 db):
      - vmaf_worker() függvény
      - VMAF_QUEUE feldolgozása
      - Minőség mérések (ab-av1 vagy ffmpeg-libvmaf)
      - CPU-intenzív, ezért csak 1 fut egyszerre
      
   f) Audio Edit Worker Thread:
      - audio_edit_worker() függvény
      - AUDIO_EDIT_QUEUE feldolgozása
      - Hangsávok eltávolítása/konvertálása
      
   g) Manual NVENC Thread:
      - process_manual_nvenc_tasks_worker()
      - Manuális újrakódolás kezelése
      - Dedikált CQ értékkel
      
   h) Video Loading Threads (pool):
      - Videók betöltése párhuzamosan
      - FFprobe adatok kinyerése
      - Felirat fájlok keresése

   i) Database Save Thread:
      - Adatbázis mentés háttérben
      - WAL mode használata
      - Batch insert optimalizáció

   QUEUE-k:
   --------
   - encoding_queue: GUI frissítések (minden worker → GUI)
   - NVENC_QUEUE: NVENC feladatok (coordinator → NVENC workers)
   - SVT_QUEUE: SVT-AV1 feladatok (coordinator → SVT worker)
   - VMAF_QUEUE: VMAF/PSNR feladatok (workers → VMAF worker)
   - AUDIO_EDIT_QUEUE: Audio művelet feladatok (GUI → audio worker)

   LOCK-ok:
   --------
   - ACTIVE_PROCESSES_LOCK: Futó subprocess-ek listájához
   - VMAF_LOCK: VMAF/PSNR worker koordinációhoz
   - CPU_WORKER_LOCK: SVT és VMAF közös CPU használat korlátozásához
   - db_lock: Adatbázis műveletek szinkronizálása


================================================================================
ADATBÁZIS MŰKÖDÉS ÉS PROBOLÁSI STRATÉGIA
================================================================================

1. HIDEGINDÍTÁS (load_videos függvény)
   ====================================
   
   DEFINÍCIÓ:
   - Nincs meglévő adatbázis bejegyzés a videókhoz
   - Első alkalommal töltjük be a videókat egy mappából
   
   FOLYAMAT:
   a) Videó fájlok feldolgozása:
      - Minden videó fájlhoz PROBOLÁS történik (ffprobe):
        * source_duration_seconds (időtartam másodpercben)
        * source_fps (képkocka/másodperc)
        * source_frame_count (összes képkocka száma)
      - Output fájlok ellenőrzése (ha léteznek):
        * output_encoder_type (nvenc/svt-av1) - Settings tag-ből
        * output_file_size_bytes (fájlméret)
        * output_modified_timestamp (módosítási dátum)
      - Source fájl stat() hívás:
        * orig_size_bytes (forrás fájl mérete)
        * source_modified_timestamp (módosítási dátum)
   
   b) Tree-be bekerülnek az adatok:
      - Minden probolt adat a GUI tree-ben jelenik meg
      - Duration, frames, fájlméret, státusz stb.
   
   c) Betöltés UTÁN háttérszálban DB mentés:
      - save_state_to_db() hívódik háttérszálban
      - FONTOS: Hidegindításnál NEM probolunk újra a save_state_to_db-ban!
      - OPTIMALIZÁCIÓ: Először összegyűjtjük az összes tree adatot egy cache-be (gyorsabb, mint egyesével)
      - Tree item mögötti eredeti adatok használata (gyors, parse-olás nélkül):
        * Először próbáljuk a tree item mögötti eredeti adatokat (self.tree_item_data[item_id])
        * source_duration_seconds, source_frame_count, source_fps -> közvetlenül, parse-olás nélkül!
        * CQ, VMAF, PSNR -> közvetlenül (float értékek), parse-olás nélkül!
        * new_size_bytes -> közvetlenül (int érték), parse-olás nélkül!
        * output_encoder_type -> közvetlenül, probolás nélkül!
        * Fallback: csak akkor parse-olunk a tree-ből, ha nincs tree item mögötti adat
      - Cache-elt stat() értékek használata (betöltéskor már stat()-oltunk):
        * Használjuk a betöltéskor cache-elt source_size_bytes értéket (nem parse-oltat, nem újra stat()-oltat!)
        * Használjuk a betöltéskor cache-elt source_modified_timestamp értéket (ha van)
        * FONTOS: Ne hívjuk meg újra a stat()-ot, ha már hívtuk betöltéskor!
        * Fallback: csak akkor stat()-olunk, ha nincs cache (ritka eset, pl. új videó vagy hiba volt)
      - Output fájlok ellenőrzése:
        * Hidegindításnál: NEM ellenőrizzük az output fájlokat (exists(), stat()), mert a betöltéskor már ellenőriztük!
        * Hidegindításnál: NEM probolunk (output_encoder_type a tree item mögötti adatokból jön)
        * Melegindításnál: csak akkor ellenőrizzük, ha van DB bejegyzés (lehet, hogy változott a fájl)
      - Batch INSERT az adatbázisba (1000 videó per batch)
      - WAL checkpoint a végén (journal fájl törlése)
   
   OPTIMALIZÁCIÓ:
   - A probolás csak a betöltéskor történik, nem a DB mentéskor
   - Tree item mögötti eredeti adatok tárolása: betöltéskor eltároljuk az eredeti adatokat (source_duration_seconds, source_frame_count, source_fps, output_encoder_type) a self.tree_item_data[item_id]-ben
   - Tree item mögötti adatok frissítése: minden művelet után (encoding, VMAF mérés) frissítjük a tree_item_data-t:
     * CQ, VMAF, PSNR értékek (float) - parse-olás nélkül
     * new_size_bytes (int) - parse-olva a new_size string-ből
     * output_encoder_type (string) - probolva, ha completed státusz
   - Tree adatok cache-elése: először összegyűjtjük az összes tree adatot egy cache-be (gyorsabb, mint egyesével)
   - Tree item mögötti adatok használata: hidegindításnál először a tree item mögötti eredeti adatokat használjuk (gyors, parse-olás nélkül!)
     * source_duration_seconds, source_frame_count, source_fps -> közvetlenül
     * CQ, VMAF, PSNR -> közvetlenül (float értékek)
     * new_size_bytes -> közvetlenül (int érték)
     * output_encoder_type -> közvetlenül
   - Fallback: csak akkor parse-olunk a tree-ből, ha nincs tree item mögötti adat (pl. új videó, vagy hiba volt)
   - Output fájlok ellenőrzése hidegindításnál kimarad (betöltéskor már ellenőriztük)


2. MELEGINDÍTÁS (load_videos függvény)
   ====================================
   
   DEFINÍCIÓ:
   - Van meglévő adatbázis bejegyzés a videókhoz
   - Korábban már betöltöttük és mentettük a videó adatokat
   
   FOLYAMAT:
   a) Adatbázisból betöltés:
      - load_state_from_db() betölti a korábbi adatokat
      - Source videó adatok (frame_count, duration, fps, size, timestamp)
      - Output videó adatok (encoder_type, size, timestamp)
      - Státusz, CQ, VMAF, PSNR értékek
   
   b) Fájl változás ellenőrzés:
      - Minden videóhoz stat() hívás (fájlméret, módosítási dátum)
      - Összehasonlítás a DB-ben mentett értékekkel:
        * Ha eltér a fájlméret (orig_size_bytes != stat().st_size) → PROBOLÁS
        * Ha eltér a módosítási dátum (>1 másodperc) → PROBOLÁS
      - Ha NEM változott → DB-ből használjuk az adatokat, NEM probolunk
   
   c) Output fájl ellenőrzés (ha completed státusz):
      - Stat() hívás (fájlméret, módosítási dátum)
      - Összehasonlítás a DB-ben mentett értékekkel:
        * Ha eltér a fájlméret vagy dátum → PROBOLÁS (encoder_type)
        * Ha NEM változott → DB-ből használjuk az encoder_type-t
   
   OPTIMALIZÁCIÓ:
   - Csak akkor probolunk, ha a fájl TÉNYLEGESEN változott
   - Stat() eredményből frissítjük a fájlméretet és timestamp-et
   - DB-ből használjuk a probolt adatokat, ha nem változott a fájl


3. START GOMB (start_encoding függvény)
   =====================================
   
   FOLYAMAT:
   a) Nem-videó fájlok másolása:
      - Képek, szövegfájlok stb. másolása a célmappába
      - Aszinkron módon, progress bar-ral
   
   b) Adatbázis mentés (save_state_to_db):
      - Ellenőrzi, hogy már fut-e DB mentés (hidegindítás után)
      - Ha fut, vár rá (max 5 perc timeout)
      - Melegindítás logikát használ:
        * Van DB bejegyzés → csak akkor probol, ha változott a fájl
        * Nincs DB bejegyzés → tree-ből olvassa az adatokat
      - Batch INSERT (1000 videó per batch)
      - WAL checkpoint a végén
   
   c) Encoding worker indítása:
      - NVENC és SVT-AV1 worker szálak indítása
      - Queue-k feltöltése a várakozó videókkal
      - Encoding folyamat indítása
   
   OPTIMALIZÁCIÓ:
   - A Start gomb után ugyanaz a logika, mint a melegindításnál
   - Csak akkor probolunk, ha a fájl ténylegesen változott
   - Tree-ből olvassuk az adatokat hidegindításnál


4. save_state_to_db PROBOLÁSI STRATÉGIA
   =====================================
   
   SOURCE VIDEÓ:
   ------------
   a) Hidegindításnál (nincs DB bejegyzés):
      - OPTIMALIZÁCIÓ: Először a tree item mögötti eredeti adatokat használjuk (gyors, parse-olás nélkül!)
        * source_duration_seconds, source_frame_count, source_fps -> közvetlenül self.tree_item_data[item_id]-ből
        * CQ, VMAF, PSNR -> közvetlenül self.tree_item_data[item_id]-ből (float értékek)
        * new_size_bytes -> közvetlenül self.tree_item_data[item_id]-ből (int érték)
      - Fallback: csak akkor parse-olunk a tree-ből, ha nincs tree item mögötti adat
      - Csak akkor probolunk, ha a tree-ben nincs adat (pl. új videó, hiba volt)
      - Cache-elt stat() értékek használata (betöltéskor már stat()-oltunk)
   
   b) Melegindításnál (van DB bejegyzés):
      - Stat() hívás → összehasonlítás DB értékekkel
      - Ha változott (méret vagy dátum) → PROBOLÁS
      - Ha NEM változott → DB-ből használjuk az adatokat
      - Stat() eredményből frissítjük a fájlméretet és timestamp-et
   
   OUTPUT VIDEÓ (ha completed státusz):
   -----------------------------------
   a) Hidegindításnál (nincs DB bejegyzés):
      - OPTIMALIZÁCIÓ: NEM ellenőrizzük az output fájlokat (exists(), stat()), mert a betöltéskor már ellenőriztük!
      - OPTIMALIZÁCIÓ: NEM probolunk (output_encoder_type a tree item mögötti adatokból jön: self.tree_item_data[item_id]['output_encoder_type'])
      - Fallback: csak akkor probolunk, ha nincs tree item mögötti adat
   
   b) Melegindításnál (van DB bejegyzés):
      - Stat() hívás → összehasonlítás DB értékekkel
      - Ha változott (méret vagy dátum) → PROBOLÁS (encoder_type)
      - Ha NEM változott → DB-ből használjuk az encoder_type-t
      - Stat() eredményből frissítjük a fájlméretet és timestamp-et
   
   OPTIMALIZÁCIÓK:
   - Hidegindításnál: Tree item mögötti eredeti adatok tárolása (betöltéskor: self.tree_item_data[item_id])
   - Minden művelet után: Tree item mögötti adatok frissítése (encoding, VMAF mérés után: CQ, VMAF, PSNR, new_size_bytes, output_encoder_type)
   - Hidegindításnál: Tree item mögötti adatok használata (gyors, parse-olás nélkül!)
   - Hidegindításnál: Tree item adatok egyszeri lekérdezése (optimalizáció: original_data csak egyszer kérdezve le)
   - Hidegindításnál: Tree adatok cache-elése (először összegyűjtjük az összes tree adatot)
   - Hidegindításnál: Output fájlok ellenőrzése kimarad (betöltéskor már ellenőriztük)
   - Hidegindításnál: Output encoder_type probolás kimarad (tree item mögötti adatokból jön)
   - Melegindításnál: Csak akkor probolunk, ha változott a fájl
   - PRAGMA beállítások tranzakció előtt (WAL mód, synchronous) - tranzakción belül nem módosítható
   - Batch INSERT (1000 videó per batch) - gyorsabb, mint egyesével
   - WAL mód és checkpoint - journal fájl törlése
   - Progress callback - látható a haladás (50 videónként vagy 2 másodpercenként)


================================================================================
KÓDOLÁSI WORKFLOW
================================================================================

5. ENCODING COORDINATOR (encoding_worker)
   =======================================
   
   FELADATA:
   - Kiválasztja a kódolandó videókat (sorrendben, "Pending" státuszú)
   - Eldönti: NVENC vagy SVT-AV1?
   - Queue-kba helyezi a feladatokat
   - Eredményeket gyűjti és feldolgozza
   - VMAF alapú CQ állítást végez
   
   FOLYAMAT:
   a) Videó kiválasztás:
      - Végignézi a tree-t sorszámok szerint
      - Csak "Pending" státuszú videókat vesz
      - Ellenőrzi: NVENC enabled? SVT enabled?
      - Auto mode: NVENC ha GPU detected, különben SVT
      
   b) Queue-ba helyezés:
      - NVENC_QUEUE.put(task) - GPU feladatokhoz
      - SVT_QUEUE.put(task) - CPU feladatokhoz
      - Task tartalmaz: video_path, item_id, target_cq/vmaf, settings
      
   c) Eredmény feldolgozás:
      - Visszakapja: success/fail, vmaf_result, output_path
      - Ha VMAF < target:
        * CQ csökkentése (vmaf_step-pel)
        * Újra queue-ba (max 10 próbálkozás)
        * Fallback SVT-ra ha NVENC nem találja a megfelelő CQ-t
      - Ha VMAF >= target:
        * "Kész" státusz beállítása
        * VMAF/PSNR mérés indítása (ha auto_vmaf_psnr enabled)
      
   d) CPU worker koordináció:
      - CPU_WORKER_LOCK használata
      - SVT és VMAF NEM futhat egyszerre (CPU)
      - Várakozás ha CPU worker aktív


6. NVENC WORKER (nvenc_worker)
   ============================
   
   FELADATA:
   - NVENC_QUEUE feldolgozása
   - GPU-alapú AV1 kódolás
   - ab-av1 CQ meghatározás (ha auto mode)
   - Validálás VirtualDub2-vel
   
   FOLYAMAT:
   a) Queue figyelés:
      - NVENC_QUEUE.get(timeout=1)
      - Várakozás új feladatra vagy stop jelre
      
   b) CQ meghatározás:
      - Ha "auto" mode: ab-av1 crf-search futtatása
        * --min-vmaf parameter
        * --max-encoded-percent parameter
        * Sample encode (gyors becslés)
        * Eredmény: optimális CQ érték
      - Ha "manual" mode: előre megadott CQ használata
      
   c) Kódolás:
      - encode_single_attempt() hívása
      - FFmpeg + av1_nvenc encoder
      - Feliratok beágyazása (ha vannak)
      - Audio kompresszió (ha enabled)
      - Átméretezés (ha enabled)
      - Settings metadata írása
      
   d) Validálás:
      - VirtualDub2 frame export (1 frame)
      - Frame count ellenőrzés (ffprobe)
      - Ha eltér >5%: "Ellenőrizendő" státusz
      - Ha rendben: vissza coordinator-nak
      
   e) Hibakezelés:
      - EncodingStopped → azonnali megszakítás
      - NoSuitableCRFFound → SVT fallback
      - Egyéb hiba → "Hiba" státusz, retry


7. SVT-AV1 WORKER (svt_worker)
   ===========================
   
   FELADATA:
   - SVT_QUEUE feldolgozása
   - CPU-alapú AV1 kódolás
   - Validálás VirtualDub2-vel
   
   FOLYAMAT (hasonló NVENC-hez):
   a) Queue figyelés: SVT_QUEUE.get(timeout=1)
   b) CQ meghatározás: ab-av1 vagy manual CRF
   c) Kódolás: FFmpeg + libsvtav1 encoder
   d) Validálás: ffprobe frame count
   e) Eredmény: vissza coordinator-nak
   
   KÜLÖNBSÉGEK NVENC-től:
   - CPU_WORKER_LOCK használata (VMAF-fel megosztva)
   - Csak 1 példány futhat (CPU korlátozás)
   - Lassabb, de univerzális (nincs GPU szükség)
   - Preset beállítás (0-13, alapértelmezett: 2)


8. VMAF/PSNR SZÁMÍTÁS (vmaf_worker)
   =================================
   
   FELADATA:
   - VMAF_QUEUE feldolgozása
   - Minőség mérések kész videókon
   - Metadata frissítése
   
   FOLYAMAT:
   a) Queue figyelés:
      - VMAF_QUEUE.get(timeout=5)
      - Csak kész videók (completed státusz)
      
   b) Mérési mód eldöntése:
      - ab-av1 preferred (gyorsabb):
        * ab-av1 vmaf --reference --distorted
        * Progress bar ab-av1 kimenetből
        * VMAF + xPSNR együtt
      - Fallback: ffmpeg-libvmaf
        * ffmpeg -lavfi libvmaf
        * Lassabb, de univerzális
      
   c) Mérés futtatása:
      - Progress callback: frissíti GUI-t
      - Interpoláció: 1%-onkénti frissítés (ha ab-av1 nem ad gyakori update-et)
      - Stop check: STOP_EVENT.is_set()
      
   d) Eredmény feldolgozás:
      - VMAF érték Settings metadata-ba
      - PSNR érték Settings metadata-ba
      - Tree frissítése: vmaf, psnr oszlopok
      - tree_item_data frissítése
      
   e) CPU koordináció:
      - CPU_WORKER_LOCK használata
      - SVT-vel megosztott CPU használat
      - Csak 1 CPU-intenzív task egyszerre


================================================================================
GUI ÉS FRISSÍTÉSI MECHANIZMUS
================================================================================

9. GUI FRISSÍTÉSEK (check_encoding_queue)
   =======================================
   
   FELADATA:
   - encoding_queue feldolgozása
   - GUI frissítések végrehajtása (thread-safe)
   - Különböző típusú üzenetek kezelése
   
   ÜZENET TÍPUSOK:
   
   a) ("nvenc_log", worker_idx, logger_idx, text):
      - NVENC worker console kimenet
      - Megfelelő tab kiválasztása (logger_idx alapján)
      - Szöve beszúrása console-ba
      
   b) ("svt_log", text):
      - SVT-AV1 worker console kimenet
      - SVT tab-ba írás
      
   c) ("update", item_id, status, cq, vmaf, psnr, progress, ...):
      - Tree item frissítése
      - Státusz, metrikák, progress oszlopok
      - tree_item_data cache frissítése
      - Completed item elrejtése (ha enabled)
      
   d) ("progress", item_id, progress_text):
      - Progress oszlop frissítése
      - Becsült befejezési idő számítása
      
   e) ("status_only", item_id, status_text):
      - Csak státusz frissítése (progress változatlan)
      
   f) ("tag", item_id, tag_name):
      - Tree item tag beállítása
      - Színezés: completed (zöld), error (piros), pending (sárga)
      
   g) ("debug_pause", ...):
      - Debug mód: megállás felhasználói inputra
      - Continue event-re várakozás
      
   h) ("save_json",):
      - DEPRECATED: már nem használt
      - Korábban JSON mentést triggerelt
      
   i) ("db_progress", message):
      - Adatbázis mentési progress
      - Státusz label frissítése
   
   FRISSÍTÉSI GYAKORISÁG:
   - 100ms-onként check (self.root.after(100, ...))
   - Több üzenet batch feldolgozása
   - Automatikus scroll (autoscroll enabled)


10. STOP MECHANIZMUSOK
    ===================
    
    KÉTFÉLE LEÁLLÍTÁS:
    
    a) Graceful Stop (stop_encoding_graceful):
       - graceful_stop_requested = True
       - Aktuális videó befejezése
       - Új videók NEM indulnak
       - Queue-k kiürítése
       - Worker-ek saját maguk lépnek ki
       - Státusz mentés adatbázisba
       
    b) Immediate Stop (stop_encoding_immediate):
       - STOP_EVENT.set()
       - Összes subprocess.terminate()
       - Worker thread-ek azonnal kilépnek
       - Queue-k nem ürülnek
       - Részleges eredmények visszaállítása "Pending"-re
       
    KOORDINÁCIÓ:
    - STOP_EVENT: threading.Event (globális)
    - ACTIVE_PROCESSES: lista futó subprocess-ekről
    - ACTIVE_PROCESSES_LOCK: thread-safe hozzáférés
    
    CLEANUP:
    - ab-av1 temp könyvtárak törlése
    - Log fájlok flush-olása
    - Adatbázis checkpoint (WAL)


================================================================================
AUDIO MŰVELETEK
================================================================================

11. AUDIO PROCESSING (audio_edit_worker)
    =====================================
    
    FELADATA:
    - AUDIO_EDIT_QUEUE feldolgozása
    - Hangsávok eltávolítása
    - 5.1 → 2.0 konverzió
    
    TÍPUSOK:
    
    a) Hangsáv eltávolítás:
       - FFmpeg -map használata
       - Kiválasztott audio stream kihagyása
       - Többi stream megtartása (videó, feliratok, egyéb audio)
       - Metadata frissítése
       
    b) 5.1 → 2.0 konverzió:
       - Két módszer:
         * "fast": pan filter (gyors, egyszerű downmix)
         * "dialogue": atempo + volume boost (párbeszéd fókusz)
       - ÚJ 2.0 sáv hozzáadása (eredeti 5.1 megtartása)
       - Nyelv kód másolása
       - Title beállítása ("2.0 Stereo")
       
    FOLYAMAT:
    - Eredeti fájl átnevezése (.original)
    - Új fájl létrehozása (FFmpeg)
    - Sikeres → eredeti törlése
    - Sikertelen → eredeti visszaállítása
    - Metadata frissítés (Settings tag)


================================================================================
FELIRAT KEZELÉS
================================================================================

12. SUBTITLE MANAGEMENT
    ====================
    
    FOLYAMAT:
    
    a) Felirat fájlok keresése:
       - find_subtitle_files(video_path)
       - Videó neve alapján (.srt, .ass, .ssa, .vtt, .sub)
       - Nyelv kód detektálás fájlnévből:
         * video.hu.srt → "hu" nyelv
         * video-eng.srt → "eng" nyelv
         * video.srt → nyelv nélkül
       
    b) Validálás:
       - is_valid_subtitle_file()
       - Fájl méret ellenőrzés (>10 byte)
       - Formátum specifikus regex-ek:
         * SRT: timecode pattern
         * VTT: WEBVTT header
         * ASS/SSA: [Events] section
         * SUB: MicroDVD frame pattern
       
    c) Nyelv normalizálás:
       - ISO 639-1/639-2 kódok
       - Fallback: "und" (undefined)
       
    d) Beágyazás kódoláskor:
       - FFmpeg -i input.srt
       - -metadata:s:s:N language=hun
       - MKV konténerbe
       
    e) Másolás:
       - Érvényes feliratok másolása output mellett
       - Érvénytelen feliratok külön (invalid_reasons)


================================================================================
HIBA KEZELÉS ÉS KIVÉTELEK
================================================================================

13. ERROR HANDLING
    ===============
    
    KIVÉTEL OSZTÁLYOK:
    
    a) EncodingStopped:
       - STOP_EVENT detektálva
       - Normál leállítás (nem hiba)
       - Cleanup nélküli kilépés
       
    b) NoSuitableCRFFound:
       - ab-av1 nem talált megfelelő CQ-t
       - Fallback: SVT-AV1 próba
       - Vagy "Ellenőrizendő" státusz
       
    c) NVENCFallbackRequired:
       - NVENC specifikus hiba
       - Automatikus SVT fallback
       
    HIBA STÁTUSZOK:
    - "Hiba" - általános kódolási hiba
    - "Ellenőrizendő" - gyanús eredmény (frame count eltérés)
    - "VMAF Hiba" - VMAF számítás sikertelen
    - "Lejátszási hiba" - VirtualDub2 nem tudta megnyitni
    
    RETRY MECHANIZMUS:
    - CQ adjustment: max 10 próbálkozás
    - Automatikus SVT fallback NVENC hiba után
    - Részleges eredmények tárolása


================================================================================
OPTIMALIZÁCIÓK ÉS TELJESÍTMÉNY
================================================================================

14. PERFORMANCE OPTIMIZATIONS
    ==========================
    
    a) Párhuzamos feldolgozás:
       - Multi-worker NVENC (1-3 GPU task párhuzamosan)
       - Dedikált SVT worker (CPU)
       - Dedikált VMAF worker (CPU)
       - Video loading pool (I/O párhuzamosítás)
       
    b) Adatbázis optimalizációk:
       - WAL mode (Write-Ahead Logging)
       - Batch INSERT (1000/batch)
       - Háttérszál mentés (nem blokkolja GUI-t)
       - Tree data cache (parse-olás elkerülése)
       
    c) FFprobe cache:
       - Hidegindítás: betöltéskor 1x probolás
       - Tree item data tárolása (parse-olás elkerülése)
       - Stat cache (file size/mtime)
       - Melegindítás: csak ha változott a fájl
       
    d) GUI frissítések:
       - Debouncing (ne minden üzenet külön)
       - Batch processing (több item egyszerre)
       - Autoscroll optimization
       - Progress interpoláció (UI responsiveness)
       
    e) Subprocess management:
       - Startup info (Windows console elrejtése)
       - Process tracking (ACTIVE_PROCESSES)
       - Timeout handling
       - Priority setting (LOW_PRIORITY)


================================================================================
KONFIGURÁCIÓS BEÁLLÍTÁSOK
================================================================================

15. CONFIGURATION PARAMETERS
    =========================
    
    a) Kódolási paraméterek:
       - min_vmaf: Minimum VMAF cél (pl. 95)
       - vmaf_step: VMAF lépésköz CQ állításnál (pl. 0.5)
       - max_encoded_percent: Max fájlméret % (pl. 75%)
       - resize_enabled: Átméretezés engedélyezése
       - resize_height: Cél magasság (pl. 1080)
       
    b) Audio beállítások:
       - audio_compression_enabled: Audio kompresszió
       - audio_compression_method: "fast" vagy "dialogue"
       - auto_51_to_stereo: Automatikus 5.1→2.0 (DEPRECATED)
       
    c) Worker beállítások:
       - nvenc_worker_count: NVENC worker szám (1-3)
       - nvenc_enabled: NVENC használata
       - svt_enabled: SVT-AV1 használata
       - svt_preset: SVT preset (0-13, default: 2)
       
    d) VMAF/PSNR:
       - auto_vmaf_psnr: Automatikus mérés kódolás után
       - Use ab-av1 for VMAF: ab-av1 preferálása ffmpeg helyett
       
    e) UI beállítások:
       - hide_completed: Kész videók elrejtése
       - autoscroll: Automatikus görgetés console-ban
       - language: 'hu' vagy 'en'
       
    f) Debug:
       - DEBUG_MODE: Debug pause-ok engedélyezése
       - LOAD_DEBUG: Betöltési debug log
       - VIDEO_LOADING_DEBUG: Részletes video loading log


================================================================================
FONTOSABB MEGJEGYZÉSEK
================================================================================

16. IMPORTANT NOTES
    ================
    
    - Hidegindításnál a probolás a BETÖLTÉSKOR történik, nem a DB mentéskor
    - A save_state_to_db-ban hidegindításnál tree-ből olvassuk az adatokat
    - Melegindításnál csak akkor probolunk, ha a fájl ténylegesen változott
    - Stat() hívások mindig történnek (fájlméret, timestamp)
    - Batch INSERT gyorsabb, mint egyesével (1000 videó per batch)
    - WAL checkpoint biztosítja, hogy a journal fájl törlődik
    - Progress callback gyakran hívódik, hogy lássuk a haladást
    - CPU_WORKER_LOCK garantálja: SVT és VMAF NEM fut egyszerre
    - STOP_EVENT thread-safe leállítást biztosít
    - tree_item_data cache csökkenti a parse/probe műveletek számát
    - Console logging thread-safe (ConsoleLogger + STDOUT_ROUTER)
    - Adatbázis műveletek lock-oltak (db_lock)
    - VirtualDub2 csak NVENC-hez kell (frame export validálás)
    - ab-av1 opcionális (fallback: manual CQ, ffmpeg-libvmaf)

================================================================================
"""

import os
import subprocess
import sys
import re
import shutil
import random
import tempfile
import platform
import signal
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Callable, Any, Union
import numpy as np
from PIL import Image
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import threading
import queue
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tkinter import scrolledtext
from contextlib import contextmanager
import ctypes
import sqlite3
import json  # FFprobe JSON kimenetéhez szükséges
from datetime import datetime
import locale
import multiprocessing
import traceback
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation

# Windows CPU prioritás beállítás
if platform.system() == 'Windows':
    try:
        PROCESS_SET_INFORMATION = 0x0200
        PROCESS_QUERY_INFORMATION = 0x0400
        BELOW_NORMAL_PRIORITY_CLASS = 0x4000
        
        kernel32 = ctypes.windll.kernel32
        
        def set_low_priority():
            """Az aktuális folyamat CPU prioritását alacsonyra állítja Windows-on"""
            handle = kernel32.OpenProcess(PROCESS_SET_INFORMATION | PROCESS_QUERY_INFORMATION, False, os.getpid())
            if handle:
                kernel32.SetPriorityClass(handle, BELOW_NORMAL_PRIORITY_CLASS)
                kernel32.CloseHandle(handle)
    except (OSError, AttributeError, ctypes.WinError):
        def set_low_priority():
            """Fallback - nem csinál semmit ha nem sikerül"""
            pass
else:
    def set_low_priority():
        """Nem-Windows rendszerek esetén nem csinál semmit"""
        pass

# Gyerekfolyamatok leállításához használt segédfüggvény
def terminate_process_tree(process):
    """Terminate a process and all its children.
    
    Recursively terminates the specified process and all its child processes
    using psutil logic (or taskkill on Windows).
    
    Args:
        process: A subprocess.Popen object or psutil.Process object, or None.
    """
    if not process:
        return
    try:
        if platform.system() == 'Windows':
            subprocess.run(
                ['taskkill', '/F', '/T', '/PID', str(process.pid)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                creationflags=subprocess.CREATE_NO_WINDOW
            )
        else:
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
    except (OSError, subprocess.SubprocessError, ProcessLookupError, AttributeError) as e:
        # Ha a taskkill/killpg nem sikerül, próbáljuk meg közvetlenül terminálni
        try:
            if process and hasattr(process, 'terminate'):
                process.terminate()
        except (OSError, ProcessLookupError, AttributeError) as e2:
            # Ha a terminate sem sikerül, csendben elnyeljük a hibát
            # (a process már lehet, hogy leállt)
            pass
# Támogatott kiterjesztések
VIDEO_EXTENSIONS = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.mpg', '.mpeg'}
SUBTITLE_EXTENSIONS = {'.srt', '.vtt', '.sub', '.ass', '.ssa'}

# Globális nyelv változó
CURRENT_LANGUAGE = 'hu'  # 'hu' vagy 'en'

# Frame/size/duration ellenőrzés toleranciái
FRAME_MISMATCH_RATIO = 0.005  # 0.5% eltérésig toleráns
FRAME_MISMATCH_MIN_DIFF = 5   # Minimum 5 frame különbség szükséges riasztáshoz
SIZE_MISMATCH_RATIO = 0.12    # 12%-nál kisebb végső méret gyanús
DURATION_MISMATCH_RATIO = 0.95  # <95% hossz esetén gyanús

# KISEBB JAVÍTÁS #16: Frame validálás konstansok
MAX_MEAN_BRIGHTNESS = 20  # Fekete frame detektáláshoz
MIN_STD_DEV = 5.0  # Fekete frame detektáláshoz
MIN_FILE_SIZE_BYTES = 10000  # Minimum fájlméret validáláshoz
MIN_FRAME_FILE_SIZE = 1000  # Minimum frame fájl méret (byte)

# Adatbázis konstansok
DB_CONNECTION_TIMEOUT = 30.0  # másodperc - SQLite kapcsolat timeout
DB_RETRY_MAX_ATTEMPTS = 3  # Maximum próbálkozások száma SQLITE_BUSY hiba esetén
DB_RETRY_DELAY = 0.1  # másodperc - Várakozás retry-ok között
DB_BATCH_SIZE = 1000  # Batch INSERT méret (videók száma)
DB_SAVE_WAIT_TIMEOUT = 300  # másodperc (5 perc) - Várakozás korábbi DB mentés befejezésére

# Subtitle validálás konstansok
SUBTITLE_VALIDATION_SAMPLE_BYTES = 2048  # Byte-ok száma felirat előnézet olvasásához

# Nyelvi szótárok
TRANSLATIONS = {
    'hu': {
        'app_title': 'AV1 Batch Video Encoder',
        'source': 'Forrás:',
        'dest': 'Cél:',
        'browse': 'Tallózás',
        'debug_mode': 'Hibakereső mód (lépésenkénti, temp megőrzés)',
        'auto_vmaf_psnr': 'Automatikus VMAF/PSNR számítás átkódolás után',
        'load_videos': 'Videók betöltése',
        'min_vmaf': 'Min VMAF:',
        'vmaf_fallback': 'VMAF csökkentés:',
        'max_encoded': 'Max átkódolt méret:',
        'resize_height': 'Átméretezés magasság:',
        'nvenc_workers': 'NVENC workerek száma:',
        'skip_av1': '.av1.mp4/.av1.mkv fájlok kihagyása (átmásolás)',
        'audio_compression': 'Hangdinamika kompresszió (5.1→2.0)',
        'audio_compression_fast': 'Gyors, mozihoz jó',
        'audio_compression_dialogue': 'Párbeszéd-központú',
        'nvenc_enabled': 'NVENC engedélyezve (40xx/50xx GPU)',
        'videos_tab': '📹 Videók',
        'nvenc_console': '🎮 NVENC konzol',
        'svt_console': '⚙️ SVT-AV1 konzol',
        'language': 'Nyelv:',
        'ffmpeg_path': 'FFmpeg helye:',
        'virtualdub_path': 'VirtualDub2 helye:',
        'abav1_path': 'ab-av1 helye:',
        'auto_detected': '(automatikusan észlelve)',
        'not_found': '(nem található)',
        'hungarian': 'Magyar',
        'english': 'English',
        'column_order': 'Sorszám',
        'column_video': 'Videó',
        'column_status': 'Státusz',
        'column_cq': 'CQ',
        'column_vmaf': 'VMAF',
        'column_psnr': 'PSNR',
        'column_progress': 'Előrehaladás',
        'column_orig_size': 'Eredeti',
        'column_new_size': 'Új',
        'column_size_change': 'Változás',
        'column_completed': 'Befejezés',
        'btn_start': 'Start',
        'btn_stop': 'Leállítás',
        'btn_immediate_stop': 'Azonnali leállítás',
        'btn_clear_table': 'Táblázat törlése',
        'btn_hide_completed': 'Elkészültek elrejtése',
        'status_ready': 'Kész áll',
        'status_nvenc_queue': 'NVENC queue-ban vár...',
        'status_svt_queue': 'SVT-AV1 queue-ban vár...',
        'status_completed': '✓ Kész',
        'status_completed_nvenc': '✓ Kész (NVENC)',
        'status_completed_svt': '✓ Kész (SVT-AV1)',
        'status_completed_copy': '✓ Kész (másolva)',
        'status_completed_exists': '✓ Kész (már létezik)',
        'status_failed': '✗ Sikertelen',
        'status_source_missing': '✗ Forrás videó hiányzik',
        'status_file_missing': '✗ Fájl hiányzik',
        'status_load_error': '✗ Betöltési hiba',
        'status_vmaf_waiting': 'VMAF ellenőrzésre vár...',
        'status_vmaf_psnr_waiting': 'VMAF/PSNR számításra vár...',
        'status_psnr_waiting': 'PSNR ellenőrzésre vár...',
        'status_vmaf_calculating': 'VMAF/PSNR számítás folyamatban...',
        'status_vmaf_only': 'VMAF számítás folyamatban...',
        'status_psnr_only': 'PSNR számítás folyamatban...',
        'status_vmaf_error': '✗ VMAF/PSNR számítás hiba',
        'status_audio_edit_queue': 'Hangsáv eltávolításra vár...',
        'status_audio_editing': 'Hangsáv eltávolítás folyamatban...',
        'status_audio_edit_done': '✓ Kész (hangsáv módosítva)',
        'status_audio_edit_failed': '✗ Hangsáv eltávolítás hiba',
        'status_nvenc_encoding': 'NVENC kódolás...',
        'status_nvenc_validation': 'NVENC validálás...',
        'status_nvenc_crf_search': 'NVENC CRF keresés...',
        'status_svt_encoding': 'SVT-AV1 kódolás...',
        'status_svt_validation': 'SVT-AV1 validálás...',
        'status_svt_crf_search': 'SVT-AV1 CRF keresés...',
        'status_needs_check': '⚠ Ellenőrizendő',
        'status_needs_check_nvenc': '⚠ Ellenőrizendő (NVENC)',
        'status_needs_check_svt': '⚠ Ellenőrizendő (SVT)',
        'menu_open': 'Megnyitás',
        'menu_source_video': 'Forrás videó',
        'menu_encoded_video': 'Átkódolt videó',
        'menu_vmaf_test': 'Teljes VMAF/PSNR ellenőrzés',
        'menu_vmaf_test_multiple': 'Teljes VMAF/PSNR ellenőrzés ({count} videó)',
        'menu_audio_tracks': 'Hangsávok',
        'menu_audio_remove_action': 'Hangsáv eltávolítása',
        'menu_audio_remove_confirm': 'Biztosan eltávolítod ezt a hangsávot?',
        'menu_audio_convert': 'Hangsáv 2.0 konverzió',
        'menu_audio_convert_confirm': 'Létrehozod a kiválasztott térhatású hangsáv 2.0 változatát?\n\n{track}\nMódszer: {method}',
        'audio_convert_title_fast': '2.0 (Gyors kompresszió)',
        'audio_convert_title_dialogue': '2.0 (Párbeszéd kiemelés)',
        'context_auto_encode': 'Automata átkódolás',
        'context_auto_reencode': 'Automata újrakódolás',
        'context_svt_encode': 'SVT-AV1 átkódolás',
        'context_svt_reencode': 'SVT-AV1 újrakódolás',
        'context_nvenc_encode': 'NVENC átkódolás',
        'context_nvenc_reencode': 'NVENC újrakódolás',
        'context_multi_encode_menu': 'Átkódolási opciók',
        'context_multi_reencode_menu': 'Újrakódolási opciók',
        'menu_vmaf_submenu': 'VMAF/PSNR ellenőrzés',
        'menu_vmaf_full': 'Teljes VMAF/PSNR ellenőrzés',
        'menu_vmaf_only': 'Csak VMAF ellenőrzés',
        'menu_psnr_only': 'Csak PSNR ellenőrzés',
        'menu_reencode': 'Újrakódolás',
        'menu_reencode_svt': 'SVT-AV1 újrakódolás',
        'msg_no_video': 'Nincs videó!',
        'msg_invalid_source': 'Érvénytelen forrás!',
        'msg_video_not_exists': 'Videó még nem létezik',
        'msg_output_not_found': 'Az átkódolt videó nem található!',
        'msg_file_info_missing': 'Nem található kimeneti fájl információ!',
        'msg_svt_already_processing': 'Ez a videó már SVT-AV1 feldolgozás alatt áll!',
        'msg_svt_reencode_confirm': 'Biztosan újrakódolod ezt a videót SVT-AV1-gyel?',
        'msg_reencode_confirm': 'Biztosan újrakódolod ezt a videót?',
        'msg_delete_failed': 'Nem sikerült törölni a meglévő fájlt:',
        'msg_svt_added': 'SVT-AV1 újrakódolás hozzáadva a sorhoz:',
        'msg_reencode_added': 'Újrakódolás hozzáadva a {encoder} queue-hoz:',
        'msg_clear_confirm': 'Biztosan törlöd az összes videót a táblázatból?',
        'msg_state_load_title': 'Előző állapot betöltése',
        'msg_state_load': 'Található előző mentett állapot!',
    },
    'en': {
        'app_title': 'AV1 Batch Video Encoder',
        'source': 'Source:',
        'dest': 'Destination:',
        'browse': 'Browse',
        'debug_mode': 'Debug mode (step-by-step, keep temp)',
        'auto_vmaf_psnr': 'Automatic VMAF/PSNR calculation after encoding',
        'load_videos': 'Load Videos',
        'min_vmaf': 'Min VMAF:',
        'vmaf_fallback': 'VMAF Reduction:',
        'max_encoded': 'Max Re-encoded Size:',
        'resize_height': 'Resize Height:',
        'nvenc_workers': 'NVENC workers:',
        'skip_av1': 'Skip .av1.mp4/.av1.mkv re-encoding (copy)',
        'audio_compression': 'Audio dynamics compression (5.1→2.0)',
        'audio_compression_fast': 'Fast, cinema-ready',
        'audio_compression_dialogue': 'Dialogue-centered',
        'nvenc_enabled': 'NVENC enabled (40xx/50xx GPU)',
        'videos_tab': '📹 Videos',
        'nvenc_console': '🎮 NVENC Console',
        'svt_console': '⚙️ SVT-AV1 Console',
        'language': 'Language:',
        'ffmpeg_path': 'FFmpeg path:',
        'virtualdub_path': 'VirtualDub2 path:',
        'abav1_path': 'ab-av1 path:',
        'auto_detected': '(auto-detected)',
        'not_found': '(not found)',
        'hungarian': 'Magyar',
        'english': 'English',
        'column_order': 'Order',
        'column_video': 'Video',
        'column_status': 'Status',
        'column_cq': 'CQ',
        'column_vmaf': 'VMAF',
        'column_psnr': 'PSNR',
        'column_progress': 'Progress',
        'column_orig_size': 'Original',
        'column_new_size': 'New',
        'column_size_change': 'Change',
        'column_completed': 'Completed',
        'btn_start': 'Start Encoding',
        'btn_stop': 'Stop',
        'btn_immediate_stop': 'Immediate Stop',
        'btn_clear_table': 'Clear Table',
        'btn_hide_completed': 'Hide Completed',
        'status_ready': 'Ready',
        'status_nvenc_queue': 'NVENC queue waiting...',
        'status_svt_queue': 'SVT-AV1 queue waiting...',
        'status_completed': '✓ Done',
        'status_completed_nvenc': '✓ Done (NVENC)',
        'status_completed_svt': '✓ Done (SVT-AV1)',
        'status_completed_copy': '✓ Done (copied)',
        'status_completed_exists': '✓ Done (already exists)',
        'status_failed': '✗ Failed',
        'status_source_missing': '✗ Source video missing',
        'status_file_missing': '✗ File missing',
        'status_load_error': '✗ Load error',
        'status_vmaf_waiting': 'VMAF check waiting...',
        'status_vmaf_psnr_waiting': 'VMAF/PSNR calculation waiting...',
        'status_psnr_waiting': 'PSNR check waiting...',
        'status_vmaf_calculating': 'VMAF/PSNR calculation in progress...',
        'status_vmaf_only': 'VMAF calculation in progress...',
        'status_psnr_only': 'PSNR calculation in progress...',
        'status_vmaf_error': '✗ VMAF/PSNR calculation error',
        'status_audio_edit_queue': 'Audio track removal queued...',
        'status_audio_editing': 'Audio track removal in progress...',
        'status_audio_edit_done': '✓ Done (audio updated)',
        'status_audio_edit_failed': '✗ Audio track removal error',
        'status_nvenc_encoding': 'NVENC encoding...',
        'status_nvenc_validation': 'NVENC validation...',
        'status_nvenc_crf_search': 'NVENC CRF search...',
        'status_svt_encoding': 'SVT-AV1 encoding...',
        'status_svt_validation': 'SVT-AV1 validation...',
        'status_svt_crf_search': 'SVT-AV1 CRF search...',
        'status_needs_check': '⚠ Needs Check',
        'status_needs_check_nvenc': '⚠ Needs Check (NVENC)',
        'status_needs_check_svt': '⚠ Needs Check (SVT)',
        'menu_open': 'Open',
        'menu_source_video': 'Source Video',
        'menu_encoded_video': 'Encoded Video',
        'menu_vmaf_test': 'Full VMAF/PSNR Check',
        'menu_vmaf_test_multiple': 'Full VMAF/PSNR Check ({count} videos)',
        'menu_audio_tracks': 'Audio Tracks',
        'menu_audio_remove_action': 'Remove audio track',
        'menu_audio_remove_confirm': 'Are you sure you want to remove this audio track?',
        'menu_audio_convert': 'Convert Surround → 2.0',
        'menu_audio_convert_confirm': 'Create a 2.0 copy from the selected surround track?\n\n{track}\nMethod: {method}',
        'audio_convert_title_fast': '2.0 (Dynamics Compression)',
        'audio_convert_title_dialogue': '2.0 (Dialogue Boost)',
        'context_auto_encode': 'Automatic encode',
        'context_auto_reencode': 'Automatic re-encode',
        'context_svt_encode': 'SVT-AV1 encode',
        'context_svt_reencode': 'SVT-AV1 re-encode',
        'context_nvenc_encode': 'NVENC encode',
        'context_nvenc_reencode': 'NVENC re-encode',
        'context_multi_encode_menu': 'Encoding options',
        'context_multi_reencode_menu': 'Re-encode options',
        'menu_vmaf_submenu': 'VMAF/PSNR Check',
        'menu_vmaf_full': 'Full VMAF/PSNR Check',
        'menu_vmaf_only': 'VMAF Check only',
        'menu_psnr_only': 'PSNR Check only',
        'menu_reencode': 'Re-encode',
        'menu_reencode_svt': 'SVT-AV1 Re-encode',
        'msg_no_video': 'No videos!',
        'msg_invalid_source': 'Invalid source!',
        'msg_video_not_exists': 'Video does not exist yet',
        'msg_output_not_found': 'Encoded video not found!',
        'msg_file_info_missing': 'Output file information not found!',
        'msg_svt_already_processing': 'This video is already being processed by SVT-AV1!',
        'msg_svt_reencode_confirm': 'Are you sure you want to re-encode this video with SVT-AV1?',
        'msg_reencode_confirm': 'Are you sure you want to re-encode this video?',
        'msg_delete_failed': 'Failed to delete existing file:',
        'msg_svt_added': 'SVT-AV1 re-encoding added to queue:',
        'msg_reencode_added': 'Re-encoding added to {encoder} queue:',
        'msg_clear_confirm': 'Are you sure you want to clear all videos from the table?',
        'msg_state_load_title': 'Load Previous State',
        'msg_state_load': 'Previous saved state found!',
    }
}

def get_default_language() -> str:
    """Get the OS default language.
    
    Returns:
        str: 'hu' if Hungarian is detected, 'en' otherwise.
    """
    try:
        # Windows
        if platform.system() == 'Windows':
            import ctypes
            windll = ctypes.windll.kernel32
            lang_id = windll.GetUserDefaultUILanguage()
            # 0x0409 = English, 0x040E = Hungarian
            if lang_id == 0x040E:
                return 'hu'
            else:
                return 'en'
        else:
            # Linux/Mac
            lang = locale.getdefaultlocale()[0]
            if lang and 'hu' in lang.lower():
                return 'hu'
            else:
                return 'en'
    except (OSError, AttributeError, ValueError, TypeError):
        return 'en'

def format_localized_number(value: Optional[Union[int, float, str]], decimals: int = 1, show_sign: bool = False) -> str:
    """Lokalizált szám formázás: magyar = tizedesvessző, angol = tizedespont
    
    Args:
        value: A formázandó szám
        decimals: Tizedesjegyek száma
        show_sign: Ha True, akkor pozitív számoknál is megjelenik a + jel
    """
    if value is None:
        return "-"
    try:
        num_value = float(value)
        if show_sign:
            formatted = f"{num_value:+.{decimals}f}"
        else:
            formatted = f"{num_value:.{decimals}f}"
        if CURRENT_LANGUAGE == 'hu':
            formatted = formatted.replace('.', ',')
        return formatted
    except (TypeError, ValueError):
        return str(value) if value is not None else "-"

def detect_nvidia_gpu():
    """Detect NVIDIA GPU and check for NVENC support (40xx/50xx series).
    
    Checks if an NVIDIA GPU is present using nvidia-smi and verifies
    if it supports NVENC encoding (specifically targeting RTX 40xx/50xx series).
    
    Returns:
        tuple: (bool, str) - (True if supported GPU found, GPU name or None)
    """
    
    def log(msg):
        """Biztonságos log írás"""
        if LOG_WRITER:
            try:
                LOG_WRITER.write(msg + "\n")
                LOG_WRITER.flush()
            except (OSError, IOError, AttributeError):
                pass
    
    log("\n=== NVIDIA GPU DETEKTÁLÁS ===")
    
    try:
        log("  nvidia-smi parancs futtatása...")
        # nvidia-smi parancs futtatása
        result = subprocess.run(['nvidia-smi', '--query-gpu=name', '--format=csv,noheader'],
                              capture_output=True, text=True, timeout=5)
        if result.returncode == 0 and result.stdout.strip():
            gpu_name = result.stdout.strip()
            log(f"  ✓ GPU található: {gpu_name}")
            # Ellenőrizzük, hogy 40xx vagy 50xx sorozatú-e
            # RTX 40xx: "RTX 40" vagy "GeForce RTX 40" vagy "RTX 4090", "RTX 4080", stb.
            # RTX 50xx: "RTX 50" vagy "GeForce RTX 50" vagy "RTX 5090", "RTX 5080", stb.
            gpu_parts = gpu_name.split()
            last_part_has_40_or_50 = (gpu_parts and ('40' in gpu_parts[-1] or '50' in gpu_parts[-1]))
            if 'RTX 40' in gpu_name or 'RTX 50' in gpu_name or last_part_has_40_or_50:
                # Ellenőrizzük pontosabban: 40xx vagy 50xx
                parts = gpu_name.split()
                for part in parts:
                    if part.startswith('40') and len(part) >= 3:  # 4090, 4080, stb.
                        log(f"  ✓ 40xx sorozat detektálva: {gpu_name}")
                        log("  ✓ NVENC engedélyezve (40xx GPU)\n")
                        return True, gpu_name
                    if part.startswith('50') and len(part) >= 3:  # 5090, 5080, stb.
                        log(f"  ✓ 50xx sorozat detektálva: {gpu_name}")
                        log("  ✓ NVENC engedélyezve (50xx GPU)\n")
                        return True, gpu_name
            log(f"  ✗ GPU nem 40xx vagy 50xx sorozatú: {gpu_name}")
            log("  ✗ NVENC nincs engedélyezve\n")
            return False, gpu_name
        else:
            log("  ✗ nvidia-smi nem adott eredményt")
    except FileNotFoundError:
        log("  ✗ nvidia-smi nem található a PATH-ban")
    except (subprocess.TimeoutExpired, subprocess.SubprocessError) as e:
        log(f"  ✗ nvidia-smi hiba: {e}")
    except Exception as e:
        log(f"  ✗ nvidia-smi váratlan hiba: {e}")
    
    # Ha nvidia-smi nem elérhető, próbáljuk Windows WMI-vel (opcionális)
    if sys.platform == 'win32':
        try:
            log("  Windows WMI próbálkozás...")
            try:
                import wmi
            except ImportError:
                log("  ✗ wmi modul nincs telepítve")
            else:
                log("  ✓ wmi modul elérhető")
                c = wmi.WMI()
                for gpu in c.Win32_VideoController():
                    if 'NVIDIA' in gpu.Name.upper():
                        gpu_name = gpu.Name
                        log(f"  ✓ NVIDIA GPU található: {gpu_name}")
                        # Ellenőrizzük, hogy 40xx vagy 50xx sorozatú-e
                        gpu_parts = gpu_name.split()
                        last_part_has_40_or_50 = (gpu_parts and ('40' in gpu_parts[-1] or '50' in gpu_parts[-1]))
                        if 'RTX 40' in gpu_name or 'RTX 50' in gpu_name or last_part_has_40_or_50:
                            parts = gpu_name.split()
                            for part in parts:
                                if part.startswith('40') and len(part) >= 3:
                                    log(f"  ✓ 40xx sorozat detektálva: {gpu_name}")
                                    log("  ✓ NVENC engedélyezve (40xx GPU)\n")
                                    return True, gpu_name
                                if part.startswith('50') and len(part) >= 3:
                                    log(f"  ✓ 50xx sorozat detektálva: {gpu_name}")
                                    log("  ✓ NVENC engedélyezve (50xx GPU)\n")
                                    return True, gpu_name
                        log(f"  ✗ GPU nem 40xx vagy 50xx sorozatú: {gpu_name}")
                        log("  ✗ NVENC nincs engedélyezve\n")
                        return False, gpu_name
        except Exception as e:
            log(f"  ✗ WMI hiba: {e}")
    
    log("  ✗ NVIDIA GPU nem található vagy nem 40xx/50xx sorozatú")
    log("  ✗ NVENC nincs engedélyezve\n")
    return False, None

def find_program_in_path(program_name):
    """Search for a program in the system PATH and common locations.
    
    Args:
        program_name: Name of the program to find (e.g., 'ffmpeg.exe').
        
    Returns:
        str: Full path to the program or None if not found.
    """
    def log(msg):
        """Biztonságos log írás"""
        if LOG_WRITER:
            try:
                LOG_WRITER.write(msg + "\n")
                LOG_WRITER.flush()
            except (OSError, IOError, AttributeError):
                pass
    
    log(f"\n=== {program_name} keresése ===")
    
    # Először próbáljuk a PATH-ban
    log(f"  PATH ellenőrzése...")
    try:
        result = subprocess.run(['where' if sys.platform == 'win32' else 'which', program_name], 
                              capture_output=True, text=True, timeout=2)
        if result.returncode == 0:
            stdout_lines = result.stdout.strip().split('\n')
            if stdout_lines and stdout_lines[0]:
                path = stdout_lines[0]
                if path and Path(path).exists():
                    log(f"  ✓ MEGTALÁLVA PATH-ban: {path}")
                    return path
        log(f"  ✗ Nem található PATH-ban")
    except Exception as e:
        log(f"  ✗ PATH ellenőrzés hiba: {e}")
    
    # Windows gyakori helyek
    if sys.platform == 'win32':
        common_paths = [
            Path(os.environ.get('ProgramFiles', 'C:\\Program Files')),
            Path(os.environ.get('ProgramFiles(x86)', 'C:\\Program Files (x86)')),
            Path(os.environ.get('LOCALAPPDATA', '')),
            Path.home() / 'AppData' / 'Local',
            Path.cwd(),
        ]
        
        program_folders = {
            'ffmpeg.exe': ['ffmpeg', 'FFmpeg'],
            'vdub64.exe': ['VirtualDub2', 'VirtualDub', 'vdub2'],
            'vdub2.exe': ['VirtualDub2', 'VirtualDub', 'vdub2'],
            'ab-av1.exe': ['ab-av1', 'ab_av1', 'abav1'],
        }
        
        # VirtualDub2 specifikus helyek ellenőrzése C:\ és D:\ gyökérben
        if program_name in ('vdub64.exe', 'vdub2.exe'):
            log(f"  VirtualDub2 specifikus helyek ellenőrzése...")
            # Legelső próbálkozás: Gyakori VirtualDub2 verziók direkt ellenőrzése
            common_vdub_paths = [
                'C:\\VirtualDub2',
                'D:\\VirtualDub2',
                'C:\\VirtualDub2_v2.4',
                'D:\\VirtualDub2_v2.4',
                'C:\\VirtualDub2_v2.5',
                'D:\\VirtualDub2_v2.5',
            ]
            for vdub_str in common_vdub_paths:
                vdub_path = Path(vdub_str)
                log(f"    Ellenőrzés: {vdub_str}")
                if vdub_path.exists() and vdub_path.is_dir():
                    prog_path = vdub_path / program_name
                    log(f"      Mappa létezik, {program_name} keresése...")
                    if prog_path.exists():
                        log(f"      ✓ MEGTALÁLVA: {prog_path}")
                        return str(prog_path)
                    else:
                        log(f"      ✗ {program_name} nem található benne")
                else:
                    log(f"      ✗ Mappa nem létezik")
            
            # Ha nem találtuk, listázzuk a gyökér mappákat
            log(f"  C:\\ és D:\\ gyökér listázása VirtualDub* mappákért...")
            for drive in ['C:\\', 'D:\\']:
                try:
                    log(f"    {drive} ellenőrzése...")
                    if os.path.exists(drive):
                        log(f"      Drive létezik, mappák listázása...")
                        items_found = []
                        for item in os.listdir(drive):
                            if item.lower().startswith('virtualdub'):
                                items_found.append(item)
                                vdub_path = Path(drive) / item
                                log(f"        Talált mappa: {item}")
                                if vdub_path.is_dir():
                                    prog_path = vdub_path / program_name
                                    if prog_path.exists():
                                        log(f"        ✓ MEGTALÁLVA: {prog_path}")
                                        return str(prog_path)
                                    else:
                                        log(f"        ✗ {program_name} nem található benne")
                        if not items_found:
                            log(f"      ✗ Nincs VirtualDub* kezdetű mappa")
                    else:
                        log(f"      ✗ Drive nem létezik")
                except (PermissionError, OSError) as e:
                    log(f"      ✗ Hozzáférési hiba: {e}")
        
        # Futtatási mappában keresés prefix alapján
        log(f"  Futtatási mappa ellenőrzése...")
        cwd = Path.cwd()
        log(f"    Aktuális mappa: {cwd}")
        if cwd.exists():
            prefix_map = {
                'ffmpeg.exe': ['ffmpeg*', 'FFmpeg*'],
                'vdub64.exe': ['virtualdub*', 'VirtualDub*'],
                'vdub2.exe': ['virtualdub*', 'VirtualDub*'],
                'ab-av1.exe': ['ab_av1*', 'ab-av1*'],
            }
            
            if program_name in prefix_map:
                patterns = prefix_map[program_name]
                log(f"    Keresési minták: {patterns}")
                for pattern in patterns:
                    try:
                        found_folders = list(cwd.glob(pattern))
                        if found_folders:
                            log(f"      Talált mappák '{pattern}' mintával: {[f.name for f in found_folders]}")
                        for folder_path in found_folders:
                            if folder_path.is_dir():
                                # Közvetlenül a mappában
                                prog_path = folder_path / program_name
                                log(f"        Ellenőrzés: {prog_path}")
                                if prog_path.exists():
                                    log(f"        ✓ MEGTALÁLVA: {prog_path}")
                                    return str(prog_path)
                                # Bin almappában
                                prog_path = folder_path / 'bin' / program_name
                                log(f"        Ellenőrzés: {prog_path}")
                                if prog_path.exists():
                                    log(f"        ✓ MEGTALÁLVA: {prog_path}")
                                    return str(prog_path)
                        if not found_folders:
                            log(f"      ✗ Nincs '{pattern}' mintával egyező mappa")
                    except Exception as e:
                        log(f"      ✗ Hiba a '{pattern}' keresése során: {e}")
        
        if program_name in program_folders:
            log(f"  Standard helyek ellenőrzése...")
            log(f"    Alap útvonalak: {[str(p) for p in common_paths]}")
            log(f"    Mappák: {program_folders[program_name]}")
            for base_path in common_paths:
                if not base_path.exists():
                    log(f"      ✗ Nem létezik: {base_path}")
                    continue
                for folder in program_folders[program_name]:
                    # Közvetlenül a mappában
                    prog_path = base_path / folder / program_name
                    log(f"        Ellenőrzés: {prog_path}")
                    if prog_path.exists():
                        log(f"        ✓ MEGTALÁLVA: {prog_path}")
                        return str(prog_path)
                    # Bin almappában
                    prog_path = base_path / folder / 'bin' / program_name
                    log(f"        Ellenőrzés: {prog_path}")
                    if prog_path.exists():
                        log(f"        ✓ MEGTALÁLVA: {prog_path}")
                        return str(prog_path)
    
    log(f"  ✗ {program_name} nem található sehol\n")
    return None

def find_virtualdub():
    """VirtualDub2 keresése - vdub64.exe vagy vdub2.exe"""
    def log(msg):
        """Biztonságos log írás"""
        if LOG_WRITER:
            try:
                LOG_WRITER.write(msg + "\n")
                LOG_WRITER.flush()
            except (OSError, IOError, AttributeError):
                pass
    
    log("\n=== VirtualDub2 keresése (vdub64.exe vagy vdub2.exe) ===")
    
    # Először próbáljuk a vdub64.exe-t
    log("  vdub64.exe keresése...")
    result = find_program_in_path('vdub64.exe')
    if result:
        log(f"  ✓ MEGTALÁLVA: {result}")
        return result
    
    # Ha nem találtuk, próbáljuk a vdub2.exe-t
    log("  vdub64.exe nem található, vdub2.exe keresése...")
    result = find_program_in_path('vdub2.exe')
    if result:
        log(f"  ✓ MEGTALÁLVA: {result}")
        return result
    
    log("  ✗ VirtualDub2 nem található (sem vdub64.exe, sem vdub2.exe)\n")
    return None

def auto_detect_programs():
    """Automatically detect external programs (FFmpeg, VirtualDub2, ab-av1).
    
    Searches for required external tools in the PATH and common locations.
    
    Returns:
        dict: Dictionary containing paths for 'ffmpeg', 'virtualdub', and 'abav1'.
    """
    return {
        'ffmpeg': find_program_in_path('ffmpeg.exe' if sys.platform == 'win32' else 'ffmpeg'),
        'virtualdub': find_virtualdub(),
        'abav1': find_program_in_path('ab-av1.exe' if sys.platform == 'win32' else 'ab-av1'),
    }

def t(key: str) -> str:
    """Translation function.
    
    Args:
        key: Translation key.
        
    Returns:
        str: Translated text or the key itself if not found.
    """
    return TRANSLATIONS.get(CURRENT_LANGUAGE, TRANSLATIONS['en']).get(key, key)

def translate_status(status_text):
    """Localize status text.
    
    Args:
        status_text: The status text to translate.
        
    Returns:
        str: Localized status text.
    """
    if not status_text:
        return status_text
    
    # Státusz fordítások
    status_map = {
        'hu': {
            'NVENC queue-ban vár...': 'status_nvenc_queue',
            'SVT-AV1 queue-ban vár...': 'status_svt_queue',
            '✓ Kész': 'status_completed',
            '✓ Kész (NVENC)': 'status_completed_nvenc',
            '✓ Kész (SVT-AV1)': 'status_completed_svt',
            '✓ Kész (másolva)': 'status_completed_copy',
            '✓ Kész (már létezik)': 'status_completed_exists',
            '✗ Sikertelen': 'status_failed',
            '✗ Forrás videó hiányzik': 'status_source_missing',
            '✗ Fájl hiányzik': 'status_file_missing',
            '✗ Betöltési hiba': 'status_load_error',
        'VMAF ellenőrzésre vár...': 'status_vmaf_waiting',
        'PSNR ellenőrzésre vár...': 'status_psnr_waiting',
        'VMAF/PSNR számításra vár...': 'status_vmaf_psnr_waiting',
        'VMAF/PSNR számítás folyamatban...': 'status_vmaf_calculating',
        'VMAF számítás folyamatban...': 'status_vmaf_only',  # Régi formátum támogatása
        'PSNR számítás folyamatban...': 'status_psnr_only',
        '✗ VMAF/PSNR számítás hiba': 'status_vmaf_error',
            '✗ VMAF számítás hiba': 'status_vmaf_error',  # Régi formátum támogatása
            'Hangsáv eltávolításra vár...': 'status_audio_edit_queue',
            'Hangsáv eltávolítás folyamatban...': 'status_audio_editing',
            '✓ Kész (hangsáv módosítva)': 'status_audio_edit_done',
            '✗ Hangsáv eltávolítás hiba': 'status_audio_edit_failed',
            'NVENC kódolás...': 'status_nvenc_encoding',
            'NVENC validálás...': 'status_nvenc_validation',
            'NVENC CRF keresés...': 'status_nvenc_crf_search',
            'SVT-AV1 kódolás...': 'status_svt_encoding',
            'SVT-AV1 validálás...': 'status_svt_validation',
            'SVT-AV1 CRF keresés...': 'status_svt_crf_search',
            '⚠ Ellenőrizendő': 'status_needs_check',
            '⚠ Ellenőrizendő (NVENC)': 'status_needs_check_nvenc',
            '⚠ Ellenőrizendő (SVT)': 'status_needs_check_svt',
        },
        'en': {
            'NVENC queue waiting...': 'status_nvenc_queue',
            'SVT-AV1 queue waiting...': 'status_svt_queue',
            '✓ Done': 'status_completed',
            '✓ Done (NVENC)': 'status_completed_nvenc',
            '✓ Done (SVT-AV1)': 'status_completed_svt',
            '✓ Done (copied)': 'status_completed_copy',
            '✓ Done (already exists)': 'status_completed_exists',
            '✗ Failed': 'status_failed',
            '✗ Source video missing': 'status_source_missing',
            '✗ File missing': 'status_file_missing',
            '✗ Load error': 'status_load_error',
        'VMAF check waiting...': 'status_vmaf_waiting',
        'PSNR check waiting...': 'status_psnr_waiting',
        'VMAF/PSNR calculation waiting...': 'status_vmaf_psnr_waiting',
        'VMAF/PSNR calculation in progress...': 'status_vmaf_calculating',
        'VMAF calculation in progress...': 'status_vmaf_only',  # Régi formátum támogatása
        'PSNR calculation in progress...': 'status_psnr_only',
            '✗ VMAF/PSNR calculation error': 'status_vmaf_error',
            '✗ VMAF calculation error': 'status_vmaf_error',  # Régi formátum támogatása
            'Audio track removal queued...': 'status_audio_edit_queue',
            'Audio track removal in progress...': 'status_audio_editing',
            '✓ Done (audio updated)': 'status_audio_edit_done',
            '✗ Audio track removal error': 'status_audio_edit_failed',
            'NVENC encoding...': 'status_nvenc_encoding',
            'NVENC validation...': 'status_nvenc_validation',
            'NVENC CRF search...': 'status_nvenc_crf_search',
            'SVT-AV1 encoding...': 'status_svt_encoding',
            'SVT-AV1 validation...': 'status_svt_validation',
            'SVT-AV1 CRF search...': 'status_svt_crf_search',
            '⚠ Needs Check': 'status_needs_check',
            '⚠ Needs Check (NVENC)': 'status_needs_check_nvenc',
            '⚠ Needs Check (SVT)': 'status_needs_check_svt',
        }
    }
    
    # Ellenőrizzük, hogy van-e fordítása
    lang_map = status_map.get(CURRENT_LANGUAGE, {})
    if status_text in lang_map:
        return t(lang_map[status_text])
    
    # Ha tartalmazza a státusz részeket, próbáljuk meg fordítani
    for orig, key in lang_map.items():
        if orig in status_text:
            return status_text.replace(orig, t(key))
    
    return status_text

def normalize_status_to_code(status_text):
    """Normalize status text to a language-independent code for database storage.
    
    Args:
        status_text: Localized status text.
        
    Returns:
        str: Internal status code or None.
    """
    if not status_text:
        return None
    
    # Státusz kódok nyelvfüggetlenül
    # FONTOS: A specifikusabb mintákat előbb kell ellenőrizni, mint az általánosabbakat!
    # A patterns listákon belül is a specifikusabb mintákat előbb kell tenni!
    status_patterns = {
        'completed_nvenc': ['✓ Kész (NVENC)', '✓ Done (NVENC)', '(NVENC)'],
        'completed_svt': ['✓ Kész (SVT-AV1)', '✓ Done (SVT-AV1)', '(SVT-AV1)'],
        'completed_copy': ['✓ Kész (másolva)', '✓ Done (copied)', '(másolva)', '(copied)'],
        'completed_exists': ['✓ Kész (már létezik)', '✓ Done (already exists)', '(már létezik)', '(already exists)'],
        'completed': ['✓ Kész (hangsáv módosítva)', '✓ Done (audio updated)', '✓ Kész', '✓ Done', 'completed'],
        'failed': ['✗ Sikertelen', '✗ Failed', 'Sikertelen', 'Failed', '✗ Hangsáv eltávolítás hiba', '✗ Audio track removal error'],
        'source_missing': ['✗ Forrás videó hiányzik', '✗ Source video missing', 'Forrás videó hiányzik', 'Source video missing'],
        'file_missing': ['✗ Fájl hiányzik', '✗ File missing', 'Fájl hiányzik', 'File missing'],
        'load_error': ['✗ Betöltési hiba', '✗ Load error', 'Betöltési hiba', 'Load error'],
        'nvenc_queue': ['NVENC queue-ban vár', 'NVENC queue waiting', 'NVENC queue', 'NVENC queue-ban vár...', 'NVENC queue waiting...'],
        'svt_queue': ['SVT-AV1 queue-ban vár', 'SVT-AV1 queue waiting', 'SVT-AV1 queue', 'SVT-AV1 queue-ban vár...', 'SVT-AV1 queue waiting...'],
        'vmaf_waiting': ['VMAF ellenőrzésre vár', 'VMAF check waiting'],
        'psnr_waiting': ['PSNR ellenőrzésre vár', 'PSNR check waiting'],
        'vmaf_psnr_waiting': ['VMAF/PSNR számításra vár', 'VMAF/PSNR calculation waiting'],
        'vmaf_calculating': ['VMAF/PSNR számítás folyamatban', 'VMAF/PSNR calculation in progress', 'VMAF számítás folyamatban', 'VMAF calculation in progress'],
        'vmaf_error': ['✗ VMAF/PSNR számítás hiba', '✗ VMAF/PSNR calculation error', '✗ VMAF számítás hiba', '✗ VMAF calculation error'],
        'audio_edit_queue': ['Hangsáv eltávolításra vár', 'Audio track removal queued'],
        'audio_editing': ['Hangsáv eltávolítás folyamatban', 'Audio track removal in progress'],
        'nvenc_encoding': ['NVENC kódolás', 'NVENC encoding'],
        'nvenc_validation': ['NVENC validálás', 'NVENC validation'],
        'nvenc_crf_search': ['NVENC CRF keresés', 'NVENC CRF search', 'NVENC CRF keresés (VMAF:', 'NVENC CRF search (VMAF:', 'NVENC CRF keresés (VMAF fallback:', 'NVENC CRF search (VMAF fallback:'],
        'svt_encoding': ['SVT-AV1 kódolás', 'SVT-AV1 encoding'],
        'svt_validation': ['SVT-AV1 validálás', 'SVT-AV1 validation'],
        'svt_crf_search': ['SVT-AV1 CRF keresés', 'SVT-AV1 CRF search', 'SVT-AV1 CRF keresés (VMAF:', 'SVT-AV1 CRF search (VMAF:', 'SVT-AV1 CRF keresés (VMAF fallback:', 'SVT-AV1 CRF search (VMAF fallback:'],
        'needs_check': ['⚠ Ellenőrizendő', '⚠ Needs Check', 'Ellenőrizendő', 'Needs Check'],
        'needs_check_nvenc': ['⚠ Ellenőrizendő (NVENC)', '⚠ Needs Check (NVENC)'],
        'needs_check_svt': ['⚠ Ellenőrizendő (SVT)', '⚠ Needs Check (SVT)'],
    }
    
        

def format_size_mb(size_bytes):
    """Format size from bytes to MB.
    
    Args:
        size_bytes: Size in bytes.
        
    Returns:
        str: Formatted size in MB (e.g., "123.4 MB") or "-" if None.
    """
    if size_bytes is None:
        return "-"
    try:
        size_mb = size_bytes / (1024 ** 2)
        size_str = format_localized_number(size_mb, decimals=1)
        return f"{size_str} MB"
    except (TypeError, ValueError):
        return "-"

def format_size_auto(size_bytes):
    """Format size from bytes to automatically selected unit (MB/GB/TB).
    
    Args:
        size_bytes: Size in bytes.
        
    Returns:
        str: Formatted size string (e.g., "1.5 GB").
    """
    if size_bytes is None:
        return "-"
    try:
        # TB (1024^4 bytes)
        if size_bytes >= (1024 ** 4):
            size_tb = size_bytes / (1024 ** 4)
            size_str = format_localized_number(size_tb, decimals=2)
            return f"{size_str} TB"
        # GB (1024^3 bytes)
        elif size_bytes >= (1024 ** 3):
            size_gb = size_bytes / (1024 ** 3)
            size_str = format_localized_number(size_gb, decimals=2)
            return f"{size_str} GB"
        # MB (1024^2 bytes)
        elif size_bytes >= (1024 ** 2):
            size_mb = size_bytes / (1024 ** 2)
            size_str = format_localized_number(size_mb, decimals=1)
            return f"{size_str} MB"
        # KB (1024 bytes)
        elif size_bytes >= 1024:
            size_kb = size_bytes / 1024
            size_str = format_localized_number(size_kb, decimals=1)
            return f"{size_str} KB"
        else:
            return f"{int(size_bytes)} B"
    except (TypeError, ValueError):
        return "-"


def normalize_number_string(num_str):
    """Convert localized number string to language-independent (English) format for DB storage.
    
    Args:
        num_str: Localized number string (e.g., "1,5").
        
    Returns:
        str: Normalized number string (e.g., "1.5").
    """
    if not num_str or num_str == "-":
        return num_str
    try:
        # Tizedesvesszőt tizedespontra cseréljük
        normalized = str(num_str).replace(',', '.')
        # Ellenőrizzük, hogy szám-e (opcionális: validálás)
        float(normalized)
        return normalized
    except (ValueError, TypeError):
        # Ha nem szám, visszaadjuk az eredeti értéket
        return num_str

def parse_size_to_bytes(size_str):
    """Parse size string to bytes, handling both localized (comma) and non-localized (dot) formats.
    
    Args:
        size_str: Size string (e.g., "1,5 GB" or "1.5 GB").
        
    Returns:
        int: Size in bytes or None if parsing fails.
    """
    try:
        if not size_str or size_str == "-":
            return None
        clean = size_str.replace("MB", "").replace("mb", "").strip()
        # Handle localized format (comma as decimal separator)
        clean = clean.replace(',', '.')
        value = float(clean)
        return int(value * (1024 ** 2))
    except (ValueError, TypeError):
        return None


def batch_scan_directory(directory):
    """Batch scan directory for all files with size and mtime.
    
    Uses os.scandir() for efficient directory traversal - 10-100× faster
    than individual stat() calls. Recursively scans subdirectories.
    
    Args:
        directory: Path object or string to directory to scan.
        
    Returns:
        dict: {Path: {'size': int, 'mtime': float}} for all files in directory tree.
              Returns empty dict on errors.
    
    Example:
        scan = batch_scan_directory(Path('/videos'))
        # {Path('/videos/video1.mp4'): {'size': 123456, 'mtime': 1234567890.0}, ...}
    """
    scan_results = {}
    
    try:
        directory_path = Path(directory) if not isinstance(directory, Path) else directory
        
        if not directory_path.exists() or not directory_path.is_dir():
            return scan_results
        
        # os.scandir() is MUCH faster than Path.iterdir() + stat()
        # because stat() info is already cached during directory iteration
        with os.scandir(directory_path) as entries:
            for entry in entries:
                try:
                    # is_file() and is_dir() are also cached - no extra I/O!
                    if entry.is_file(follow_symlinks=False):
                        # entry.stat() reuses already cached stat info - very fast!
                        stat_info = entry.stat()
                        scan_results[Path(entry.path)] = {
                            'size': stat_info.st_size,
                            'mtime': stat_info.st_mtime
                        }
                    elif entry.is_dir(follow_symlinks=False):
                        # Recursive scan for subdirectories
                        try:
                            sub_results = batch_scan_directory(Path(entry.path))
                            scan_results.update(sub_results)
                        except (OSError, PermissionError):
                            # Skip inaccessible subdirectories
                            continue
                except (OSError, PermissionError):
                    # Skip inaccessible files
                    continue
    except (OSError, PermissionError):
        # Return whatever we managed to scan
        pass
    
    return scan_results


def is_directory_completely_empty(directory):
    """Check if a directory is missing or completely empty (recursively contains no files).
    
    Args:
        directory: Path to the directory.
        
    Returns:
        bool: True if directory is empty or missing, False otherwise.
    """
    try:
        path = Path(directory)
    except (TypeError, ValueError, OSError):
        return True
    try:
        if not path.exists():
            return True
        for _, _, files in os.walk(path):
            if files:
                return False
        return True
    except (OSError, PermissionError):
        return False


def status_code_to_localized(code):
    """Translate status code to localized text.
    
    Args:
        code: Internal status code.
        
    Returns:
        str: Localized status text.
    """
    if not code:
        return t('status_nvenc_queue')  # Default
    
    code_map = {
        'completed': 'status_completed',
        'completed_nvenc': 'status_completed_nvenc',
        'completed_svt': 'status_completed_svt',
        'completed_copy': 'status_completed_copy',
        'completed_exists': 'status_completed_exists',
        'failed': 'status_failed',
        'source_missing': 'status_source_missing',
        'file_missing': 'status_file_missing',
        'load_error': 'status_load_error',
        'nvenc_queue': 'status_nvenc_queue',
        'svt_queue': 'status_svt_queue',
        'vmaf_waiting': 'status_vmaf_waiting',
        'psnr_waiting': 'status_psnr_waiting',
        'vmaf_psnr_waiting': 'status_vmaf_psnr_waiting',
        'vmaf_calculating': 'status_vmaf_calculating',
        'vmaf_error': 'status_vmaf_error',
        'nvenc_encoding': 'status_nvenc_encoding',
        'nvenc_validation': 'status_nvenc_validation',
        'nvenc_crf_search': 'status_nvenc_crf_search',
        'svt_encoding': 'status_svt_encoding',
        'svt_validation': 'status_svt_validation',
        'svt_crf_search': 'status_svt_crf_search',
        'needs_check': 'status_needs_check',
        'needs_check_nvenc': 'status_needs_check_nvenc',
        'needs_check_svt': 'status_needs_check_svt',
        'audio_edit_queue': 'status_audio_edit_queue',
        'audio_editing': 'status_audio_editing',
    }
    
    return t(code_map.get(code, 'status_nvenc_queue'))

def is_status_completed(status_text):
    """Check if the status indicates completion (language-independent).
    
    Args:
        status_text: Status text or code.
        
    Returns:
        bool: True if status is completed.
    """
    if not status_text:
        return False
    status_code = normalize_status_to_code(status_text)
    return status_code in ('completed', 'completed_nvenc', 'completed_svt', 'completed_copy', 'completed_exists')

def is_status_failed(status_text):
    """Check if the status indicates failure (language-independent).
    
    Args:
        status_text: Status text or code.
        
    Returns:
        bool: True if status is failed.
    """
    if not status_text:
        return False
    status_code = normalize_status_to_code(status_text)
    return status_code in ('failed', 'source_missing', 'file_missing', 'vmaf_error', 'load_error')

def is_status_queue(status_text):
    """Check if the status indicates waiting in queue (language-independent).
    
    Args:
        status_text: Status text or code.
        
    Returns:
        bool: True if status is queued.
    """
    if not status_text:
        return False
    status_code = normalize_status_to_code(status_text)
    return status_code in ('nvenc_queue', 'svt_queue', 'vmaf_waiting', 'psnr_waiting', 'vmaf_psnr_waiting', 'audio_edit_queue')

def get_completed_status_for_encoder(encoder_name):
    """Get localized 'Completed' status based on encoder name.
    
    Args:
        encoder_name: Name of the encoder (e.g., 'NVENC', 'SVT').
        
    Returns:
        str: Localized completed status text.
    """
    if "SVT" in encoder_name or "svt" in encoder_name.lower():
        return t('status_completed_svt')
    elif "NVENC" in encoder_name:
        return t('status_completed_nvenc')
    else:
        return t('status_completed')

# Nyelvkód mapping
LANGUAGE_MAP = {
    'en': 'eng', 'hu': 'hun', 'de': 'ger', 'fr': 'fre', 'es': 'spa', 'it': 'ita',
    'pt': 'por', 'ru': 'rus', 'ja': 'jpn', 'ko': 'kor', 'zh': 'chi', 'ar': 'ara',
    'pl': 'pol', 'nl': 'dut', 'sv': 'swe', 'no': 'nor', 'da': 'dan', 'fi': 'fin',
    'cs': 'cze', 'sk': 'slo', 'ro': 'rum', 'tr': 'tur', 'el': 'gre', 'he': 'heb',
    'hi': 'hin', 'th': 'tha', 'vi': 'vie', 'uk': 'ukr', 'bg': 'bul', 'hr': 'hrv', 'sr': 'srp',
    'eng': 'eng', 'hun': 'hun', 'ger': 'ger', 'deu': 'ger', 'fra': 'fre', 'fre': 'fre',
    'esp': 'spa', 'spa': 'spa', 'ita': 'ita', 'por': 'por', 'rus': 'rus', 'jpn': 'jpn',
    'kor': 'kor', 'chi': 'chi', 'zho': 'chi', 'ara': 'ara', 'pol': 'pol', 'nld': 'dut',
    'dut': 'dut', 'swe': 'swe', 'nor': 'nor', 'dan': 'dan', 'fin': 'fin', 'ces': 'cze',
    'cze': 'cze', 'slk': 'slo', 'slo': 'slo', 'ron': 'rum', 'rum': 'rum', 'tur': 'tur',
    'ell': 'gre', 'gre': 'gre', 'heb': 'heb', 'hin': 'hin', 'tha': 'tha', 'vie': 'vie',
    'ukr': 'ukr', 'bul': 'bul', 'hrv': 'hrv', 'srp': 'srp',
}

# Globális debug állapot
DEBUG_MODE = False

# Globális SVT-AV1 lock és queue
SVT_LOCK = threading.Lock()
SVT_QUEUE = queue.Queue()

# VMAF/PSNR tesztelés queue és lock
VMAF_LOCK = threading.Lock()
VMAF_QUEUE = queue.Queue()

# Hangsáv-módosítás queue
AUDIO_EDIT_QUEUE = queue.Queue()

# NVENC queue - több workeres NVENC kódoláshoz
NVENC_QUEUE = queue.Queue()

# KRITIKUS: Közös CPU worker lock - biztosítja, hogy csak 1 CPU worker (SVT-AV1 vagy VMAF/PSNR) fusson egyszerre
CPU_WORKER_LOCK = threading.Lock()

# KRITIKUS: NVENC worker lock - biztosítja, hogy csak 1 NVENC worker fusson egyszerre (már nem használjuk, mert több worker van)
# NVENC_WORKER_LOCK = threading.Lock()  # Megjegyzés: több workeres megoldásnál nincs szükség lock-ra

# Globális aktív processek tárolása (thread-safe)
ACTIVE_PROCESSES = []
ACTIVE_PROCESSES_LOCK = threading.Lock()

# Globális leállítás esemény és kivétel
STOP_EVENT = threading.Event()

# Aktuális GUI példány tárolása a globális függvényekhez
GUI_INSTANCE = None

# Külső eszközök alapértelmezett elérési útjai
DEFAULT_FFMPEG = 'ffmpeg.exe' if os.name == 'nt' else 'ffmpeg'
DEFAULT_FFPROBE = 'ffprobe.exe' if os.name == 'nt' else 'ffprobe'
DEFAULT_ABAV1 = 'ab-av1.exe' if os.name == 'nt' else 'ab-av1'

FFMPEG_PATH = DEFAULT_FFMPEG
FFPROBE_PATH = DEFAULT_FFPROBE
ABAV1_PATH = DEFAULT_ABAV1
VDUB2_PATH = None
LIBVMAF_SUPPORTS_PSNR = True


def apply_external_tool_paths(ffmpeg_path=None, abav1_path=None, virtualdub_path=None):
    """Set global tool paths based on provided arguments.
    
    Updates the global variables for external tool paths (FFmpeg, FFprobe, ab-av1, VirtualDub).
    If a path is provided, it validates and sets the corresponding global variable.
    FFprobe path is automatically derived from FFmpeg path if possible.
    
    Args:
        ffmpeg_path: Path to FFmpeg executable (optional).
        abav1_path: Path to ab-av1 executable (optional).
        virtualdub_path: Path to VirtualDub2 executable (optional).
        
    Returns:
        None. Modifies global variables FFMPEG_PATH, FFPROBE_PATH, ABAV1_PATH, VDUB2_PATH.
    """
    global FFMPEG_PATH, FFPROBE_PATH, ABAV1_PATH, VDUB2_PATH

    if ffmpeg_path:
        ffmpeg_path = os.fspath(ffmpeg_path)
        FFMPEG_PATH = ffmpeg_path
        ffmpeg_path_obj = Path(ffmpeg_path)
        ffprobe_name = 'ffprobe.exe' if ffmpeg_path_obj.suffix.lower() == '.exe' else 'ffprobe'
        ffprobe_candidate = ffmpeg_path_obj.with_name(ffprobe_name)
        if ffprobe_candidate.exists():
            FFPROBE_PATH = os.fspath(ffprobe_candidate)
        else:
            FFPROBE_PATH = DEFAULT_FFPROBE
    else:
        FFMPEG_PATH = FFMPEG_PATH or DEFAULT_FFMPEG
        FFPROBE_PATH = FFPROBE_PATH or DEFAULT_FFPROBE

    if abav1_path:
        ABAV1_PATH = os.fspath(abav1_path)
    else:
        ABAV1_PATH = DEFAULT_ABAV1

    if virtualdub_path:
        vdub_path = Path(virtualdub_path)
        VDUB2_PATH = vdub_path if vdub_path.exists() else None
    else:
        VDUB2_PATH = None

# Globális log writer (pythonw.exe kompatibilis)
LOG_WRITER = None


def _str_to_bool(value):
    """Convert string or environment variable value to boolean.
    
    Args:
        value: String value to convert (typically from environment variable).
        
    Returns:
        bool: True if value is "1", "true", "yes", or "on" (case-insensitive),
              False otherwise (including None).
    """
    if value is None:
        return False
    return str(value).strip().lower() in ("1", "true", "yes", "on")


LOAD_DEBUG = _str_to_bool(os.environ.get("AV1_LOAD_DEBUG"))
VIDEO_LOADING_DEBUG = _str_to_bool(os.environ.get("AV1_VIDEO_LOADING_DEBUG"))

# Video loading log fájl
VIDEO_LOADING_LOG = None
VIDEO_LOADING_LOG_LOCK = threading.Lock()

def init_video_loading_log():
    """Video loading log fájl inicializálása"""
    global VIDEO_LOADING_LOG
    if VIDEO_LOADING_DEBUG and VIDEO_LOADING_LOG is None:
        try:
            VIDEO_LOADING_LOG = open("videoloading.log", "w", encoding="utf-8")
            VIDEO_LOADING_LOG.write(f"=== Video Loading Debug Log Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")
            VIDEO_LOADING_LOG.flush()
        except Exception as e:
            print(f"Warning: Could not create videoloading.log: {e}")

def video_loading_log(message):
    """Ultra részletes video loading log (--videoloading esetén)."""
    if not VIDEO_LOADING_DEBUG:
        return
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]  # milliszekundumokkal
    line = f"[{timestamp}] {message}"
    try:
        print(line)
    except Exception:
        pass
    with VIDEO_LOADING_LOG_LOCK:
        if VIDEO_LOADING_LOG:
            try:
                VIDEO_LOADING_LOG.write(line + "\n")
                VIDEO_LOADING_LOG.flush()
            except (OSError, IOError, AttributeError):
                pass

def video_loading_log_json(data, title="DB Content"):
    """DB tartalom JSON formátumban írása a videoloading.log fájlba."""
    if not VIDEO_LOADING_DEBUG:
        return
    try:
        # JSON formázás indentálással (olvashatóbb)
        json_str = json.dumps(data, indent=2, ensure_ascii=False, default=str)
        with VIDEO_LOADING_LOG_LOCK:
            if VIDEO_LOADING_LOG:
                try:
                    VIDEO_LOADING_LOG.write(f"\n=== {title} (JSON) ===\n")
                    VIDEO_LOADING_LOG.write(json_str)
                    VIDEO_LOADING_LOG.write(f"\n=== End of {title} ===\n\n")
                    VIDEO_LOADING_LOG.flush()
                except (OSError, IOError, AttributeError):
                    pass
    except Exception as e:
        # Ha a JSON serializálás nem sikerül, csak egy hibaüzenetet írunk
        video_loading_log(f"ERROR: Could not serialize {title} to JSON: {e}")

def load_debug_log(message):
    """Betöltés közbeni hibakereső log (AV1_LOAD_DEBUG=1 esetén)."""
    if not LOAD_DEBUG:
        return
    timestamp = datetime.now().strftime("%H:%M:%S")
    line = f"[LOAD-DEBUG {timestamp}] {message}"
    try:
        print(line)
    except Exception:
        pass
    log_writer = globals().get('LOG_WRITER')
    if log_writer:
        try:
            log_writer.write(line + "\n")
            log_writer.flush()
        except (OSError, IOError, AttributeError):
            pass


class EncodingStopped(Exception):
    """Jelzi, hogy felhasználói leállítás történt."""
    pass


class NoSuitableCRFFound(Exception):
    """Jelzi, hogy ab-av1 nem talált megfelelő CRF értéket (VMAF >= 85 ÉS fájl <= 75%)."""
    pass


class NVENCFallbackRequired(Exception):
    """Jelzi, hogy az NVENC átállítása SVT-AV1-re szükséges."""
    pass


def resolve_encoding_defaults(initial_min_vmaf, vmaf_step, max_encoded_percent):
    """Biztosítja, hogy a VMAF értékek a GUI csúszkáinak aktuális állapotát kövessék."""
    gui = GUI_INSTANCE

    if gui is not None:
        if initial_min_vmaf is None:
            initial_min_vmaf = getattr(gui, "current_min_vmaf", None)
            if initial_min_vmaf is None:
                initial_min_vmaf = float(gui.min_vmaf.get())
        if vmaf_step is None:
            vmaf_step = getattr(gui, "current_vmaf_step", None)
            if vmaf_step is None:
                vmaf_step = float(gui.vmaf_step.get())
        if max_encoded_percent is None:
            max_encoded_percent = getattr(gui, "current_max_encoded_percent", None)
            if max_encoded_percent is None:
                max_encoded_percent = int(gui.max_encoded_percent.get())
    else:
        if initial_min_vmaf is None:
            initial_min_vmaf = 95.0
        if vmaf_step is None:
            vmaf_step = 2.5
        if max_encoded_percent is None:
            max_encoded_percent = 75

    return float(initial_min_vmaf), float(vmaf_step), int(max_encoded_percent)

def get_startup_info():
    """Create Windows subprocess startupinfo to hide console window.
    
    Returns:
        subprocess.STARTUPINFO: Configuration to hide console on Windows,
                                 None on other platforms.
    """
    if os.name != "nt":
        return None
    
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = subprocess.SW_HIDE
    return startupinfo

@contextmanager
def managed_subprocess(cmd, cwd=None, stop_event=None, timeout=180):
    """Run a subprocess with management for stopping and timeouts.
    
    Args:
        cmd: Command list to execute.
        cwd: Current working directory.
        stop_event: Event to check for stop requests.
        timeout: Timeout in seconds.
        
    Returns:
        subprocess.CompletedProcess or None on failure/stop.
    """
    """
    Context manager for subprocess.Popen that ensures proper cleanup.
    
    Args:
        cmd: Command to execute
        cwd: Working directory
        stop_event: Optional threading.Event to signal cancellation
        timeout: Timeout in seconds
    
    Yields:
        subprocess.Popen process object
    
    Raises:
        EncodingStopped: If stop_event is set
        subprocess.TimeoutExpired: If process exceeds timeout
    """
    process = None
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1,
            cwd=cwd,
            startupinfo=get_startup_info()
        )
        
        # Process regisztráció
        with ACTIVE_PROCESSES_LOCK:
            ACTIVE_PROCESSES.append(process)
        
        yield process
        
    finally:
        # Cleanup garantálása
        if process:
            try:
                if process.poll() is None:  # Még fut
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
            except (OSError, ProcessLookupError, subprocess.SubprocessError):
                # Ha a terminate/kill nem sikerül, próbáljuk meg kill-tel
                try:
                    if process.poll() is None:
                        process.kill()
                        process.wait(timeout=2)
                except (OSError, ProcessLookupError, subprocess.TimeoutExpired):
                    pass
            finally:
                # Process törlése a listából
                with ACTIVE_PROCESSES_LOCK:
                    if process in ACTIVE_PROCESSES:
                        ACTIVE_PROCESSES.remove(process)

def sanitize_path(path: Path) -> str:
    """
    Sanitize path to prevent injection attacks and ensure it exists.
    
    Args:
        path: Path object to sanitize
    
    Returns:
        Sanitized path string
    
    Raises:
        FileNotFoundError: If path does not exist
        ValueError: If path is not a file or is a symlink (security risk)
    """
    if not path:
        raise ValueError("Path cannot be None or empty")
    
    try:
        resolved = path.resolve()
    except (OSError, RuntimeError) as e:
        raise ValueError(f"Cannot resolve path: {e}") from e
    
    # Ellenőrizzük, hogy a path létezik
    if not resolved.exists():
        raise FileNotFoundError(f"Path does not exist: {resolved}")
    
    # Biztonsági ellenőrzés: ne engedjük a szimbolikus linkeket (opcionális, de biztonságosabb)
    # Megjegyzés: Ez lehet, hogy túl szigorú, de biztonságosabb
    try:
        if resolved.is_symlink():
            # Ha szimbolikus link, követjük, de figyelmeztetünk
            target = resolved.readlink()
            resolved = target.resolve()
    except (OSError, RuntimeError):
        # Ha nem sikerül, folytatjuk a resolved path-dal
        pass
    
    return os.fspath(resolved)

def cleanup_ab_av1_temp_dirs(base_dir):
    """Eltávolítja az ab-av1 által hátrahagyott .ab-av1- kezdetű mappákat."""
    try:
        base_path = Path(base_dir)
        if not base_path.exists():
            return
        for entry in base_path.iterdir():
            if entry.is_dir() and entry.name.startswith(".ab-av1-"):
                try:
                    shutil.rmtree(entry, ignore_errors=True)
                except Exception as e:
                    print(f"⚠ ab-av1 temp törlés hiba: {e}")
    except Exception as e:
        print(f"⚠ ab-av1 temp bejárás hiba: {e}")

class ConsoleLogger:
    """Thread-safe konzol logger tkinter Text widget-hez és log fájlhoz"""

    def __init__(self, text_widget, gui_queue, log_file=None, log_files_list=None, logger_index=0):
        self.text_widget = text_widget
        self.gui_queue = gui_queue
        self.log_file = log_file  # Log fájl objektum (backward compatibility)
        self.log_files_list = log_files_list  # Lista a log fájlokról (logger_index alapján választás)
        self.logger_index = logger_index  # Logger objektum saját indexe (nem változik)
        self.encoder_type = None  # 'nvenc' vagy 'svt'
        self.buffer = ""  # Buffer a részleges sorokhoz
        self.worker_index = 0  # Worker index (a queue üzenetekhez)

    def set_encoder_type(self, encoder_type):
        self.encoder_type = encoder_type

    def set_worker_index(self, worker_index):
        try:
            self.worker_index = int(worker_index)
        except (ValueError, TypeError):
            self.worker_index = 0

    def _get_log_file(self):
        """Logger index alapján visszaadja a megfelelő log fájlt"""
        # Ha van log_files_list, akkor logger_index alapján választunk (nem worker_index!)
        # Ez biztosítja, hogy minden logger objektum mindig ugyanabba a log fájlba írjon,
        # függetlenül attól, hogy melyik worker használja
        if self.log_files_list and self.encoder_type == 'nvenc':
            if len(self.log_files_list) > 0:
                # Logger index alapján választunk, hogy elkerüljük a race condition-t
                log_file_idx = self.logger_index % len(self.log_files_list)
                return self.log_files_list[log_file_idx]
        # Backward compatibility: ha nincs lista, az eredeti log_file-t használjuk
        return self.log_file

    def write(self, message):
        """Thread-safe írás a konzolra és log fájlba - új sor kezelés javítással"""
        if not message:
            return

        message = message.replace('\r', '\n')

        # Log fájlba írás (logger_index alapján választott fájlba)
        log_file = self._get_log_file()
        if log_file:
            try:
                log_file.write(message)
                log_file.flush()
            except (OSError, IOError, AttributeError):
                pass

        # Buffer hozzáadása
        self.buffer += message

        # Teljes sorok feldolgozása
        lines = self.buffer.split('\n')
        self.buffer = lines.pop()  # Utolsó részleges sort megtartjuk

        # Teljes sorok küldése - CSAK a saját encoder típusának megfelelő konzolra
        for line in lines:
            payload = line + '\n'
            if self.encoder_type == 'nvenc':
                # Küldjük a worker_index-et és a logger_index-et is, hogy a konzol kiválasztásánál
                # a logger_index-et használhassuk (ami nem változik, így elkerüljük a race condition-t)
                self.gui_queue.put(("nvenc_log", self.worker_index, self.logger_index, payload))
            elif self.encoder_type == 'svt':
                self.gui_queue.put(("svt_log", payload))
            else:
                # Default fallback
                self.gui_queue.put(("nvenc_log", self.worker_index, self.logger_index, payload))

    def flush(self):
        """Buffer ürítése - CSAK a saját encoder típusának megfelelő konzolra"""
        if self.buffer:
            payload = self.buffer + '\n'
            # Log fájlba írás (logger_index alapján választott fájlba)
            log_file = self._get_log_file()
            if log_file:
                try:
                    log_file.write(self.buffer)
                    log_file.flush()
                except (OSError, IOError, AttributeError):
                    pass
            if self.encoder_type == 'nvenc':
                # Küldjük a worker_index-et és a logger_index-et is
                self.gui_queue.put(("nvenc_log", self.worker_index, self.logger_index, payload))
            elif self.encoder_type == 'svt':
                self.gui_queue.put(("svt_log", payload))
            else:
                # Default fallback
                self.gui_queue.put(("nvenc_log", self.worker_index, self.logger_index, payload))
            self.buffer = ""

class ThreadSafeStdoutRouter:
    """Szálanként elkülönített stdout átirányítás."""

    def __init__(self, fallback_stream):
        self._fallback = fallback_stream
        self._local = threading.local()
        self.encoding = getattr(fallback_stream, 'encoding', None)
        self.errors = getattr(fallback_stream, 'errors', None)

    def set_logger(self, logger):
        stack = self._get_stack()
        stack.append(logger)

    def clear_logger(self):
        stack = self._get_stack()
        if stack:
            stack.pop()

    def _current_logger(self):
        stack = self._get_stack()
        if stack:
            return stack[-1]
        return None

    def _get_stack(self):
        stack = getattr(self._local, 'logger_stack', None)
        if stack is None:
            stack = []
            self._local.logger_stack = stack
        return stack

    def write(self, message):
        logger = self._current_logger()
        if logger is not None:
            logger.write(message)
        else:
            if self._fallback is not None:
                try:
                    self._fallback.write(message)
                except (AttributeError, OSError, IOError):
                    # Ha a fallback nem írható, csendben elnyeljük
                    pass

    def flush(self):
        logger = self._current_logger()
        if logger is not None and hasattr(logger, 'flush'):
            logger.flush()
        else:
            if self._fallback is not None:
                try:
                    self._fallback.flush()
                except (AttributeError, OSError, IOError):
                    # Ha a fallback nem flush-olható, csendben elnyeljük
                    pass

    def isatty(self):
        if self._fallback is not None:
            try:
                return self._fallback.isatty()
            except (AttributeError, OSError, IOError):
                return False
        return False

    def fileno(self):
        if self._fallback is not None:
            try:
                return self._fallback.fileno()
            except (AttributeError, OSError, IOError):
                raise OSError("fileno() not available")
        raise OSError("fileno() not available")

    def __getattr__(self, item):
        if self._fallback is not None:
            return getattr(self._fallback, item)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{item}'")

# Biztonságos stdoutinicializálás - ha sys.stdout None, akkor használunk egy dummy stream-et
_safe_stdout = sys.stdout
if _safe_stdout is None:
    # Ha sys.stdout None, létrehozunk egy dummy stream-et
    class DummyStream:
        """Dummy stdout stream for environments where sys.stdout is None (e.g., pythonw.exe)."""
        def write(self, s): pass
        def flush(self): pass
        def isatty(self): return False
        def fileno(self): raise OSError("fileno() not available")
    _safe_stdout = DummyStream()

STDOUT_ROUTER = ThreadSafeStdoutRouter(_safe_stdout)
sys.stdout = STDOUT_ROUTER

@contextmanager
def console_redirect(logger):
    """Context manager to redirect print() output to a specific logger.
    
    Args:
        logger: Logger instance to redirect output to.
        
    Yields:
        None. Restores original stdout routing on exit.
    """
    STDOUT_ROUTER.set_logger(logger)
    try:
        yield
    finally:
        STDOUT_ROUTER.clear_logger()

def debug_pause(current_step, next_step, file_info=""):
    """
    Debug mód - thread-safe verzió.
    Queue-n keresztül küldi az üzenetet a fő thread-nek.
    """
    if not DEBUG_MODE:
        return
    
    print(f"\n{'!'*80}")
    print(f"🛑 DEBUG MEGÁLLÁS")
    print(f"{'!'*80}")
    print(f"Jelenlegi: {current_step}")
    print(f"Következő: {next_step}")
    if file_info:
        print(f"Info: {file_info}")
    print(f"{'!'*80}\n")
    
    continue_event = threading.Event()
    
    try:
        # Ha van GUI queue (adva az encoding_worker által)
        if hasattr(debug_pause, 'gui_queue'):
            debug_pause.gui_queue.put((
                "debug_pause",
                current_step,
                next_step,
                file_info,
                continue_event
            ))
            # Várunk a fő thread-re - timeout 30 másodperc
            if not continue_event.wait(timeout=30.0):
                print("    ⚠ DEBUG: Timeout - a GUI nem válaszolt 30 másodpercen belül, folytatás...")
        else:
            # Konzolos fallback
            input("▶ DEBUG - ENTER a folytatáshoz...")
    except Exception as e:
        print(f"Debug pause hiba: {e}")
        try:
            input("▶ DEBUG - ENTER a folytatáshoz...")
        except (EOFError, KeyboardInterrupt, OSError):
            pass

def extract_language_from_filename(filename):
    """Extract language code from filename using common patterns.
    
    Args:
        filename: Filename string.
        
    Returns:
        tuple: (base_name, language_code) or (filename, None) if not found.
    """
    basename = os.path.basename(filename)
    name_parts = basename.rsplit('.', 1)
    name_without_ext = name_parts[0] if len(name_parts) > 1 else basename
    
    patterns = [
        (r'^(.+?)[-]([a-z]{2}-[A-Z]{2})$', '-'),
        (r'^(.+?)[.]([a-z]{2}-[A-Z]{2})$', '.'),
        (r'^(.+?)[-]([A-Za-z]{2,})$', '-'),
        (r'^(.+?)[_]([A-Za-z]{2,})$', '_'),
        (r'^(.+?)[.]([A-Za-z]{2,3})$', '.'),
        (r'^(.+?)\s+([A-Za-z]{2,})\s*$', ' '),
    ]
    
    for pattern, separator in patterns:
        match = re.match(pattern, name_without_ext, re.IGNORECASE)
        if match:
            base_name = match.group(1).strip()
            lang_part = match.group(2).strip()
            lang_normalized = lang_part.lower()
            if '-' in lang_normalized:
                lang_normalized = lang_normalized.split('-')[0]
            if lang_normalized in LANGUAGE_MAP:
                return (base_name, lang_part)
    return (name_without_ext, None)

def normalize_language_code(lang_string):
    """Normalize language string to ISO 639-1 code if possible.
    
    Args:
        lang_string: Language string (e.g., 'eng', 'hu', 'en-US').
        
    Returns:
        str: Normalized language code (e.g., 'en') or 'und' if unknown.
    """
    if not lang_string:
        return 'und'
    lang_clean = lang_string.strip().lower()
    if '-' in lang_clean:
        lang_clean = lang_clean.split('-')[0]
    if lang_clean in LANGUAGE_MAP:
        return LANGUAGE_MAP[lang_clean]
    return 'und'

def find_subtitle_files(video_path):
    """Find subtitle files associated with a video.
    
    Finds all subtitle files (.srt, .ass, .ssa) that match the video name,
    optionally with a language code.
    
    Args:
        video_path: Path to the video file (Path).
        
    Returns:
        list: List of (subtitle_path, language_code) tuples.
    """
    video_stem = video_path.stem
    video_dir = video_path.parent
    subtitle_files = []
    found_paths = set()
    
    for file_path in video_dir.iterdir():
        if not file_path.is_file() or file_path.suffix.lower() not in SUBTITLE_EXTENSIONS:
            continue
        base_name, lang_part = extract_language_from_filename(file_path.name)
        if video_stem.strip().lower() == base_name.strip().lower():
            if file_path not in found_paths:
                subtitle_files.append((file_path, lang_part))
                found_paths.add(file_path)
    return subtitle_files

SUBTITLE_VALIDATION_SAMPLE_BYTES = 200_000
SRT_BLOCK_PATTERN = re.compile(r'^\s*\d+\s*\r?\n\s*\d{2}:\d{2}:\d{2},\d{3}\s*-->\s*\d{2}:\d{2}:\d{2},\d{3}', re.MULTILINE)
VTT_HEADER_PATTERN = re.compile(r'^\ufeff?WEBVTT', re.IGNORECASE)
VTT_TIMECODE_PATTERN = re.compile(r'\d{2}:\d{2}:\d{2}\.\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}\.\d{3}')
ASS_EVENTS_PATTERN = re.compile(r'\[Events\]', re.IGNORECASE)
ASS_DIALOGUE_PATTERN = re.compile(r'^\s*Dialogue:', re.IGNORECASE | re.MULTILINE)
SUB_MICRODVD_PATTERN = re.compile(r'\{\d+\}\{\d+\}')

def _read_subtitle_preview(file_path, limit=SUBTITLE_VALIDATION_SAMPLE_BYTES):
    """Read a small preview of the subtitle file for validation.
    
    Args:
        file_path: Path to the subtitle file.
        limit: Number of bytes to read.
        
    Returns:
        str: Decoded text content or empty string on error.
    """
    try:
        with file_path.open('rb') as handle:
            data = handle.read(limit)
    except (OSError, IOError):
        return ""
    if not data:
        return ""
    text = data.decode('utf-8', errors='ignore')
    if not text.strip():
        text = data.decode('latin-1', errors='ignore')
    return text.replace('\x00', '')

def is_valid_subtitle_file(file_path):
    """Validate subtitle file content and format.
    
    Checks file size, readability, and format-specific headers/patterns
    (SRT, VTT, ASS/SSA, SUB).
    
    Args:
        file_path: Path to the subtitle file.
        
    Returns:
        tuple: (bool, str) - (True if valid, error message or empty string).
    """
    suffix = file_path.suffix.lower()
    try:
        stat_info = file_path.stat()
    except (OSError, ValueError):
        return False, "Felirat nem olvasható"
    if stat_info.st_size < 10:
        return False, "Felirat fájl túl kicsi (<10 bájt)"
    text_preview = _read_subtitle_preview(file_path)
    if not text_preview or not text_preview.strip():
        return False, "Felirat üres vagy nem értelmezhető"
    if suffix == '.srt':
        if SRT_BLOCK_PATTERN.search(text_preview):
            return True, ""
        return False, "Hiányzó SRT időzítés"
    if suffix == '.vtt':
        first_line = text_preview.splitlines()[0] if text_preview.splitlines() else ""
        if VTT_HEADER_PATTERN.search(first_line) or VTT_TIMECODE_PATTERN.search(text_preview):
            return True, ""
        return False, "WEBVTT fejléc/időzítés hiányzik"
    if suffix in ('.ass', '.ssa'):
        if ASS_EVENTS_PATTERN.search(text_preview) and ASS_DIALOGUE_PATTERN.search(text_preview):
            return True, ""
        return False, "ASS/SSA Events rész hiányzik"
    if suffix == '.sub':
        if SUB_MICRODVD_PATTERN.search(text_preview):
            return True, ""
        return False, "SUB időzítés hiányzik"
    return True, ""

def split_valid_invalid_subtitles(subtitle_files):
    """Split subtitle files into valid and invalid groups.
    
    Args:
        subtitle_files: List of (subtitle_path, language_code) tuples.
        
    Returns:
        tuple: (valid, invalid) where:
            - valid: List of valid subtitles [(path, language)]
            - invalid: List of invalid subtitles [(path, language, reason)]
    """
    valid = []
    invalid = []
    for sub_path, lang_part in subtitle_files:
        is_valid, reason = is_valid_subtitle_file(sub_path)
        if is_valid:
            valid.append((sub_path, lang_part))
        else:
            invalid.append((sub_path, lang_part, reason or "Ismeretlen formátum"))
    return valid, invalid

def copy_video_and_subtitles(source_video_path, dest_video_path):
    """Copy video file and its associated subtitles to the destination.
    
    Also handles copying of valid subtitles found next to the source video.
    
    Args:
        source_video_path: Source video path.
        dest_video_path: Destination video path.
        
    Returns:
        bool: True if successful, False otherwise.
    """
    # Videófájl és hozzá tartozó feliratok másolása.

    # Ellenőrizzük, hogy a cél videó már létezik-e
    if dest_video_path.exists():
        print(f"⚠ Videó már létezik a célhelyen, kihagyás: {dest_video_path.name}")
        return False
    
    try:
        # Videó másolása
        dest_video_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"📋 Videó másolása: {source_video_path.name}")
        shutil.copy2(source_video_path, dest_video_path)
        print(f"✓ Videó másolva: {dest_video_path}")
        
        # Feliratok keresése és másolása
        subtitle_files = find_subtitle_files(source_video_path)
        if subtitle_files:
            for sub_path, lang_part in subtitle_files:
                # Felirat cél útvonalának kiszámítása
                dest_sub_name = dest_video_path.stem
                if lang_part:
                    dest_sub_name += f".{lang_part}"
                dest_sub_name += sub_path.suffix
                dest_sub_path = dest_video_path.parent / dest_sub_name
                
                print(f"📋 Felirat másolása: {sub_path.name}")
                shutil.copy2(sub_path, dest_sub_path)
                print(f"✓ Felirat másolva: {dest_sub_path}")
        
        return True
    except Exception as e:
        print(f"✗ Másolási hiba: {e}")
        # Ha a videó másolása sikertelen volt, töröljük a részbeni eredményt
        if dest_video_path.exists():
            try:
                dest_video_path.unlink()
            except (OSError, PermissionError):
                pass
        return False


def copy_video_fallback(source_path, dest_path, subtitle_files, logger=None):
    """Copy video unchanged when encoding fails (preserves original extension).
    
    Used when no suitable CRF can be found for encoding. Copies source video
    to destination with original extension, plus all valid subtitles.
    
    Args:
        source_path: Source video path (Path).
        dest_path: Destination path with original extension (Path).
        subtitle_files: List of (subtitle_path, language) tuples.
        logger: Logger instance for console output (ConsoleLogger).
        
    Returns:
        bool: True if copy successful, False otherwise.
    """
    try:
        if logger:
            with console_redirect(logger):
                print(f"\n{'='*80}")
                print(f"📋 VÁLTOZATLAN MÁSOLÁS (nincs megfelelő CRF)")
                print(f"{'='*80}")
                print(f"Forrás: {source_path}")
                print(f"Cél: {dest_path}")
                print(f"Eredeti kiterjesztés megtartva: {source_path.suffix}")
        
        # Copy video file
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, dest_path)
        
        # Copy valid subtitles
        subtitle_count = 0
        for sub_path, lang in subtitle_files:
            dest_sub = dest_path.parent / sub_path.name
            shutil.copy2(sub_path, dest_sub)
            subtitle_count += 1
            if logger:
                with console_redirect(logger):
                    lang_display = f" ({lang})" if lang else ""
                    print(f"  ✓ Felirat másolva: {sub_path.name}{lang_display}")
        
        if logger:
            with console_redirect(logger):
                size_mb = dest_path.stat().st_size / (1024**2)
                size_str = format_localized_number(size_mb, decimals=1)
                print(f"\n✓ Másolás sikeres: {dest_path.name}")
                print(f"  Méret: {size_str} MB")
                if subtitle_count:
                    print(f"  Feliratok: {subtitle_count} db")
                print(f"{'='*80}\n")
        
        return True
        
    except Exception as e:
        if logger:
            with console_redirect(logger):
                print(f"✗ Másolás hiba: {e}")
        return False


def copy_non_video_files(source_root, dest_root, progress_callback=None):
    """Copy non-video files (nfo, jpg, png, txt) from source to destination.
    
    Args:
        source_root: Source root directory.
        dest_root: Destination root directory.
        progress_callback: Optional callback for progress updates.
    """
    if not dest_root:
        return 0
    source_path = Path(source_root)
    dest_path = Path(dest_root)
    copied_count = 0
    
    try:
        source_resolved = source_path.resolve()
    except OSError:
        source_resolved = source_path
    try:
        dest_resolved = dest_path.resolve()
    except OSError:
        dest_resolved = dest_path
    
    if source_resolved == dest_resolved:
        if progress_callback:
            progress_callback("Forrás és cél azonos, nem-videó másolás kihagyva.")
        return 0
    
    skip_dest_subtree = False
    try:
        dest_resolved.relative_to(source_resolved)
        skip_dest_subtree = True
    except ValueError:
        skip_dest_subtree = False
    
    # Először megszámoljuk az összes fájlt (optimalizálva, hogy ne blokkolja a GUI-t)
    all_files = []
    file_count = 0
    last_count_update = time.time()
    start_time = time.time()
    
    # Optimalizálás: iteratív bejárás, hogy ne blokkolja a GUI-t
    try:
        # Használjuk az os.walk-ot rglob helyett, mert jobban kezelhető
        for root, dirs, files in os.walk(source_path):
            root_path = Path(root)
            for file_name in files:
                source_file = root_path / file_name
                
                try:
                    current_resolved = source_file.resolve()
                except OSError:
                    current_resolved = source_file
                
                if skip_dest_subtree and (current_resolved == dest_resolved or dest_resolved in current_resolved.parents):
                    continue
                
                if (source_file.suffix.lower() not in VIDEO_EXTENSIONS and 
                    source_file.suffix.lower() not in SUBTITLE_EXTENSIONS):
                    all_files.append(source_file)
                    file_count += 1
                    # Másodpercenként frissítjük a számlálást, hogy ne blokkolja a GUI-t
                    current_time = time.time()
                    if current_time - last_count_update >= 1.0:  # 1 másodperces frissítés
                        if progress_callback:
                            elapsed_str = f"{int(current_time - start_time)}s"
                            progress_callback(("copy_progress", file_count, 0, f"Nem-videó fájlok keresése... ({file_count} találat, {elapsed_str})"))
                        last_count_update = current_time
    except Exception as e:
        if progress_callback:
            progress_callback(("copy_error", f"✗ Hiba fájlok keresésekor: {e}"))
        return 0
    
    total_files = len(all_files)
    processed_count = 0
    copied_count = 0
    skipped_count = 0
    last_update_time = time.time()
    update_interval = 0.5  # Fél másodperces frissítés
    
    if total_files == 0:
        if progress_callback:
            progress_callback(("copy_done", 0, 0, "Nincs másolandó nem-videó fájl."))
        return 0
    
    if progress_callback:
        progress_callback(("copy_start", total_files, 0, f"Nem-videó fájlok másolása... (0/{total_files})"))
    
    for source_file in all_files:
        try:
            current_resolved = source_file.resolve()
        except OSError:
            current_resolved = source_file
        
        relative_path = source_file.relative_to(source_path)
        dest_file = dest_path / relative_path
        
        try:
            dest_file.parent.mkdir(parents=True, exist_ok=True)
        except (OSError, PermissionError) as e:
            if progress_callback:
                progress_callback(("copy_error", f"✗ Hiba mappa létrehozásakor: {relative_path.parent} - {e}"))
            continue
        
        processed_count += 1
        
        # Csak akkor másolunk, ha nem létezik vagy régebbi
        should_copy = False
        if not dest_file.exists():
            should_copy = True
        elif source_file.stat().st_mtime > dest_file.stat().st_mtime:
            should_copy = True
        else:
            skipped_count += 1
        
        # Frissítés fél másodpercenként
        current_time = time.time()
        if current_time - last_update_time >= update_interval:
            if progress_callback:
                if should_copy:
                    status_text = f"Feldolgozás: {relative_path.name[:50]}... ({processed_count}/{total_files})"
                else:
                    status_text = f"Kihagyva (már létezik): {relative_path.name[:50]}... ({processed_count}/{total_files})"
                progress_callback(("copy_progress", total_files, processed_count, status_text))
            last_update_time = current_time
        
        if should_copy:
            try:
                shutil.copy2(source_file, dest_file)
                copied_count += 1
            except (OSError, PermissionError, shutil.Error) as e:
                if progress_callback:
                    progress_callback(("copy_error", f"✗ Hiba másoláskor: {relative_path.name[:50]}... - {e}"))
    
    # Utolsó frissítés
    if progress_callback:
        progress_callback(("copy_done", total_files, processed_count, f"✓ {copied_count} fájl másolva, {skipped_count} már létezett. ({total_files} összesen)"))
    
    return copied_count


def calculate_psnr_only(reference_path, encoded_path, stop_event=None, logger=None):
    """Calculate PSNR metric using FFmpeg.
    
    Args:
        reference_path: Path to reference video.
        encoded_path: Path to encoded video.
        stop_event: Threading event to stop calculation.
        logger: Logger instance.
        
    Returns:
        float: PSNR value or None on error.
    """

    if stop_event is None:
        stop_event = STOP_EVENT
    
    reference_str = os.fspath(reference_path.absolute())
    encoded_str = os.fspath(encoded_path.absolute())
    
    psnr_cmd = [
        FFMPEG_PATH,
        '-i', reference_str,
        '-i', encoded_str,
        '-lavfi', 'psnr',
        '-f', 'null',
        '-'
    ]
    
    if logger:
        logger.write(f"\n{'='*80}\n")
        logger.write(f"?? PSNR SZÁMÍTÁS (külön futtatás)\n")
        logger.write(f"{'='*80}\n")
        logger.write(' '.join(psnr_cmd) + '\n')
        logger.write(f"{'='*80}\n\n")
        logger.flush()
    
    try:
        process = subprocess.Popen(
            psnr_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            startupinfo=get_startup_info()
        )
        
        with ACTIVE_PROCESSES_LOCK:
            ACTIVE_PROCESSES.append(process)
        
        psnr_value = None
        try:
            for line in process.stdout:
                if stop_event.is_set():
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                    raise EncodingStopped()
                
                if logger:
                    logger.write(line)
                    logger.flush()
                
                avg_match = re.search(r'(?:avg|average)[:=]\s*([\d.]+)', line, re.IGNORECASE)
                if avg_match:
                    try:
                        psnr_value = float(avg_match.group(1))
                    except ValueError:
                        pass
        except EncodingStopped:
            raise
        finally:
            with ACTIVE_PROCESSES_LOCK:
                if process in ACTIVE_PROCESSES:
                    ACTIVE_PROCESSES.remove(process)
        
        try:
            process.wait()
        except (OSError, subprocess.SubprocessError) as e:
            if logger:
                logger.write(f"PSNR process wait hiba: {e}\n")
                logger.flush()
        
        if process.returncode != 0:
            error_msg = f"✗ PSNR számítás hiba (return code: {process.returncode})"
            if logger:
                logger.write(error_msg + '\n')
                logger.flush()
            print(error_msg)
            return psnr_value
        
        return psnr_value
    except EncodingStopped:
        raise
    except Exception as e:
        error_msg = f"✗ PSNR számítás hiba: {e}"
        if logger:
            logger.write(error_msg + '\n')
            logger.flush()
        print(error_msg)
        return None

def _is_abav1_available():
    """Eldönti, hogy az AB-AV1 futtatható-e."""
    if not ABAV1_PATH:
        return False
    try:
        ab_path = Path(ABAV1_PATH)
        if ab_path.exists():
            return True
    except (OSError, ValueError):
        pass
    return shutil.which(ABAV1_PATH) is not None


def _parse_eta_to_seconds(eta_text):
    """Parse FFmpeg ETA text (HH:MM:SS) to seconds.
    
    Args:
        eta_text: ETA string.
        
    Returns:
        int: Seconds or None.
    """

    if not eta_text:
        return None
    text = str(eta_text).strip().lower()
    if not text or text in {'n/a', 'na', 'nan', '-'}:
        return None

    # HH:MM:SS vagy MM:SS formátum
    if re.match(r'^\d{1,2}:\d{2}:\d{2}$', text):
        hours, minutes, seconds = (int(part) for part in text.split(':'))
        return hours * 3600 + minutes * 60 + seconds
    if re.match(r'^\d{1,2}:\d{2}$', text):
        minutes, seconds = (int(part) for part in text.split(':'))
        return minutes * 60 + seconds

    total_seconds = 0.0
    matched = False
    for value, unit in re.findall(r'(\d+(?:\.\d+)?)\s*(hours?|hrs?|h|minutes?|mins?|m|seconds?|secs?|s)', text):
        matched = True
        numeric_value = float(value)
        if unit.startswith('h'):
            total_seconds += numeric_value * 3600
        elif unit.startswith('m'):
            total_seconds += numeric_value * 60
        else:
            total_seconds += numeric_value
    if matched:
        return total_seconds

    # Egyszerűen megadott szám (feltételezzük, hogy másodperc)
    try:
        return float(text)
    except ValueError:
        return None


def _format_eta_short(seconds_value):
    """Rövid, könnyen olvasható ETA formátum."""
    if seconds_value is None:
        return "n/a"
    seconds_value = max(0, int(seconds_value))
    if seconds_value >= 3600:
        hours = seconds_value // 3600
        minutes = (seconds_value % 3600) // 60
        return f"{hours}h {minutes}m"
    if seconds_value >= 60:
        minutes = seconds_value // 60
        seconds = seconds_value % 60
        return f"{minutes}m {seconds}s"
    return f"{seconds_value}s"


def format_seconds_hms(total_seconds):
    """HH:MM:SS formátumban adja vissza a másodperc értéket (None, ha nem értelmezhető)."""
    if total_seconds is None:
        return None
    try:
        total_seconds = int(max(0, round(float(total_seconds))))
    except (ValueError, TypeError):
        return None
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def format_metric_value(value, decimals=2):
    """Közös formázás VMAF/PSNR értékekhez (round half up) - lokalizálva."""
    if value is None:
        return "-"
    try:
        quant = Decimal('1').scaleb(-decimals)
        decimal_value = Decimal(str(value))
        rounded = decimal_value.quantize(quant, rounding=ROUND_HALF_UP)
        formatted = format(rounded, f'.{decimals}f')
        # Lokalizálás: magyar = tizedesvessző, angol = tizedespont
        if CURRENT_LANGUAGE == 'hu':
            formatted = formatted.replace('.', ',')
        return formatted
    except (InvalidOperation, ValueError, TypeError):
        try:
            formatted = f"{float(value):.{decimals}f}"
            if CURRENT_LANGUAGE == 'hu':
                formatted = formatted.replace('.', ',')
            return formatted
        except (ValueError, TypeError):
            return str(value)


def _run_abav1_metric(metric, reference_path, encoded_path, progress_callback=None, stop_event=None, logger=None, duration_seconds=None):
    """Run an ab-av1 metric command (vmaf or xpsnr) and return the result.
    
    Args:
        metric: 'vmaf' or 'xpsnr'.
        reference_path: Path to reference video.
        encoded_path: Path to encoded video.
        progress_callback: Callback for progress updates.
        stop_event: Event to stop calculation.
        logger: Logger instance.
        duration_seconds: Video duration in seconds.
        
    Returns:
        float: Metric score or None on error.
    """
    if stop_event is None:
        stop_event = STOP_EVENT
    if stop_event.is_set():
        raise EncodingStopped()

    metric_name = metric_name.lower()
    reference_str = os.fspath(reference_path.absolute())
    encoded_str = os.fspath(encoded_path.absolute())
    cmd = [
        ABAV1_PATH,
        metric_name,
        '--reference', reference_str,
        '--distorted', encoded_str,
    ]

    try:
        duration_seconds = float(duration_seconds)
        if duration_seconds <= 0:
            duration_seconds = None
    except (TypeError, ValueError):
        duration_seconds = None

    if progress_callback:
        progress_callback(f"{metric_name.upper()} (ab-av1)")

    if logger:
        logger.write(f"\n{'='*80}\n")
        logger.write(f"ab-av1 {metric_name.upper()} számítás indítva\n")
        logger.write(f"{' '.join(cmd)}\n")
        logger.write(f"{'='*80}\n")
        logger.flush()

    interpolator_stop = threading.Event()
    interpolator_lock = threading.Lock()
    interpolation_state = {
        'percent': None,
        'eta_seconds': None,
        'update_time': None,
        'finish_time': None,
        'reset_requested': False,
    }

    def _send_progress(percent=None, eta_seconds=None, eta_text=None, interpolated=False, done=False):
        if not progress_callback:
            return
        elapsed_seconds = None
        if percent is not None and duration_seconds:
            try:
                elapsed_seconds = max(0.0, min(duration_seconds, duration_seconds * (percent / 100.0)))
            except (TypeError, ValueError):
                elapsed_seconds = None
        payload = {
            'type': 'abav1_progress',
            'metric': metric_name.upper(),
            'percent': percent,
            'eta_seconds': eta_seconds,
            'eta_text': eta_text,
            'duration_seconds': duration_seconds,
            'interpolated': interpolated,
            'done': done,
            'timestamp': time.time(),
            'elapsed_seconds': elapsed_seconds,
        }
        display_eta = eta_text
        if display_eta is None and eta_seconds is not None:
            display_eta = _format_eta_short(eta_seconds)
        if percent is not None:
            percent_display = f"{format_localized_number(percent, decimals=1)}%"
        else:
            percent_display = "?"
        if display_eta is not None:
            payload['text'] = f"{metric_name.upper()} {percent_display} (ETA {display_eta})"
        else:
            payload['text'] = f"{metric_name.upper()} {percent_display}"
        try:
            progress_callback(payload)
        except Exception:
            # A GUI callback hibája ne állítsa le a fő folyamatot
            pass

    def _start_interpolator():
        if not progress_callback:
            return None

        def _interpolate_progress():
            last_sent_percent = None
            while not interpolator_stop.wait(1):
                if stop_event.is_set():
                    break
                with interpolator_lock:
                    percent = interpolation_state.get('percent')
                    finish_time = interpolation_state.get('finish_time')
                    update_time = interpolation_state.get('update_time')
                    reset_requested = interpolation_state.get('reset_requested', False)
                    if reset_requested:
                        interpolation_state['reset_requested'] = False
                if reset_requested:
                    last_sent_percent = None
                if percent is None or finish_time is None or update_time is None:
                    continue
                now_monotonic = time.monotonic()
                total_window = finish_time - update_time
                if total_window <= 0:
                    continue
                elapsed = now_monotonic - update_time
                if elapsed <= 0:
                    continue
                elapsed_clamped = min(elapsed, total_window)
                remaining_fraction = 1.0 - (elapsed_clamped / total_window)
                projected_percent = min(percent + (1 - remaining_fraction) * (100 - percent), 99.9)
                if last_sent_percent is not None and projected_percent - last_sent_percent < 0.1:
                    continue
                last_sent_percent = projected_percent
                eta_seconds = max(finish_time - now_monotonic, 0)
                _send_progress(percent=projected_percent, eta_seconds=eta_seconds, interpolated=True)

        thread = threading.Thread(target=_interpolate_progress, name="abav1-progress", daemon=True)
        thread.start()
        return thread

    creationflags = 0
    preexec_fn = None
    if platform.system() == 'Windows':
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        preexec_fn = os.setsid

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        startupinfo=get_startup_info(),
        creationflags=creationflags,
        preexec_fn=preexec_fn
    )

    with ACTIVE_PROCESSES_LOCK:
        ACTIVE_PROCESSES.append(process)

    interpolator_thread = _start_interpolator()
    full_output = []
    last_percent_reported = None
    try:
        for line in process.stdout:
            if stop_event.is_set():
                terminate_process_tree(process)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    terminate_process_tree(process)
                raise EncodingStopped()
            full_output.append(line)
            if logger:
                logger.write(line)
            percent_match = re.search(r'(\d+)%.*eta\s+(.+)', line, re.IGNORECASE)
            if percent_match:
                try:
                    percent = int(percent_match.group(1))
                except ValueError:
                    percent = None
                eta_text = percent_match.group(2).strip()
                eta_seconds = _parse_eta_to_seconds(eta_text)
                now_monotonic = time.monotonic()
                if percent is not None:
                    last_percent_reported = percent
                with interpolator_lock:
                    prev_percent = interpolation_state.get('percent')
                    prev_time = interpolation_state.get('update_time')
                    estimated_eta = eta_seconds
                    if estimated_eta is None and percent is not None and prev_percent is not None and prev_time is not None and percent > prev_percent:
                        elapsed = now_monotonic - prev_time
                        percent_delta = percent - prev_percent
                        if elapsed > 0 and percent_delta > 0:
                            remaining = max(100 - percent, 0)
                            estimated_eta = (elapsed / percent_delta) * remaining
                    interpolation_state['percent'] = percent
                    interpolation_state['eta_seconds'] = estimated_eta
                    interpolation_state['update_time'] = now_monotonic
                    if estimated_eta is not None and percent is not None:
                        interpolation_state['finish_time'] = now_monotonic + estimated_eta
                    else:
                        interpolation_state['finish_time'] = None
                    interpolation_state['reset_requested'] = True
                _send_progress(percent=percent, eta_seconds=eta_seconds, eta_text=eta_text)
        process.wait()
    finally:
        with ACTIVE_PROCESSES_LOCK:
            if process in ACTIVE_PROCESSES:
                ACTIVE_PROCESSES.remove(process)
        interpolator_stop.set()
        if interpolator_thread:
            interpolator_thread.join(timeout=2)

    if process.returncode != 0:
        raise RuntimeError(f"ab-av1 {metric_name} parancs hibával tért vissza (rc={process.returncode})")

    output_text = ''.join(full_output)
    metric_value = _extract_abav1_metric_value(metric_name, output_text)
    if metric_value is None:
        raise RuntimeError(f"Nem sikerült kiolvasni az ab-av1 {metric_name} eredményét.")

    if progress_callback:
        final_percent = last_percent_reported if last_percent_reported is not None else 100
        _send_progress(percent=100 if final_percent >= 100 else final_percent, eta_seconds=0, eta_text="kész", done=True)
        progress_callback(f"{metric_name.upper()}: {format_metric_value(metric_value)}")

    if logger:
        logger.write(f"{metric_name.upper()} eredmény: {metric_value:.4f}\n")
        logger.flush()

    return metric_value


def _extract_abav1_metric_value(metric_name, output_text):
    """Megpróbálja kinyerni a metrika értékét az ab-av1 kimenetéből."""
    lines = output_text.splitlines()
    numeric_line_pattern = re.compile(r'^\s*([0-9]+(?:\.[0-9]+)?)\s*$')
    # Először keresünk egy olyan sort, ami csak a számot tartalmazza (ab-av1 gyakran így zár)
    for line in reversed(lines):
        match = numeric_line_pattern.match(line)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                continue

    keywords = [metric_name.lower()]
    if metric_name.lower() == 'xpsnr':
        keywords.append('psnr')
    for line in reversed(lines):
        lower = line.lower()
        if any(key in lower for key in keywords):
            matches = re.findall(r'([0-9]+(?:\.[0-9]+)?)', line)
            for match in reversed(matches):
                try:
                    return float(match)
                except ValueError:
                    continue
    return None


def calculate_full_vmaf(reference_path, encoded_path, progress_callback=None, stop_event=None, logger=None, check_vmaf=True, check_psnr=True, metric_done_callback=None):
    """Calculate VMAF and/or PSNR metrics using ab-av1 (preferred) or FFmpeg fallback.
    
    Args:
        reference_path: Path to reference video.
        encoded_path: Path to encoded video.
        progress_callback: Callback for progress updates.
        stop_event: Event to stop calculation.
        logger: Logger instance.
        check_vmaf: Whether to calculate VMAF.
        check_psnr: Whether to calculate PSNR.
        metric_done_callback: Callback when a metric is done.
        
    Returns:
        tuple: (vmaf_value, psnr_value)
    """
    """Teljes VMAF + XPSNR számítás ab-av1 segítségével, FFmpeg-es visszaeséssel."""
    if stop_event is None:
        stop_event = STOP_EVENT
    reference_path = Path(reference_path)
    encoded_path = Path(encoded_path)
    if not check_vmaf and not check_psnr:
        check_vmaf = True

    duration_seconds, _ = get_video_info(reference_path)
    if _is_abav1_available():
        try:
            vmaf_value = None
            psnr_value = None
            if check_vmaf:
                vmaf_value = _run_abav1_metric('vmaf', reference_path, encoded_path, progress_callback, stop_event, logger, duration_seconds=duration_seconds)
                if metric_done_callback and vmaf_value is not None:
                    metric_done_callback('VMAF', vmaf_value)
            if check_psnr:
                psnr_value = _run_abav1_metric('xpsnr', reference_path, encoded_path, progress_callback, stop_event, logger, duration_seconds=duration_seconds)
                if metric_done_callback and psnr_value is not None:
                    metric_done_callback('PSNR', psnr_value)
            return vmaf_value, psnr_value
        except EncodingStopped:
            raise
        except Exception as e:
            fallback_msg = f"⚠ ab-av1 VMAF/XPSNR számítás hiba: {e} – FFmpeg fallback"
            print(fallback_msg)
            if logger:
                logger.write(fallback_msg + "\n")
                logger.flush()

    vmaf_value, psnr_value = _calculate_full_vmaf_ffmpeg(reference_path, encoded_path, progress_callback, stop_event, logger)
    if metric_done_callback:
        if check_vmaf and vmaf_value is not None:
            metric_done_callback('VMAF', vmaf_value)
        if check_psnr and psnr_value is not None:
            metric_done_callback('PSNR', psnr_value)
    if not check_vmaf:
        vmaf_value = None
    if not check_psnr:
        psnr_value = None
    return vmaf_value, psnr_value


def _calculate_full_vmaf_ffmpeg(reference_path, encoded_path, progress_callback=None, stop_event=None, logger=None):
    """Calculate VMAF using FFmpeg directly (fallback method).
    
    Args:
        reference_path: Path to reference video.
        encoded_path: Path to encoded video.
        progress_callback: Callback for progress updates.
        stop_event: Event to stop calculation.
        logger: Logger instance.
        
    Returns:
        tuple: (vmaf_value, psnr_value) - PSNR is always None in this fallback.
    """
    """
    Teljes VMAF és PSNR teszt futtatása ffmpeg libvmaf használatával.
    Visszaadja a (VMAF érték, PSNR érték) tuple-t vagy (None, None)-t hiba esetén.
    FFmpeg 8.0+ kompatibilis - VMAF és PSNR egy parancsban.
    """
    global LIBVMAF_SUPPORTS_PSNR
    if stop_event is None:
        stop_event = STOP_EVENT
    
    reference_str = os.fspath(reference_path.absolute())
    encoded_str = os.fspath(encoded_path.absolute())
    
    # Videó teljes hosszának lekérdezése
    duration_seconds, _ = get_video_info(reference_path)
    if duration_seconds is None:
        duration_seconds = 0
    
    duration_hours = int(duration_seconds // 3600)
    duration_mins = int((duration_seconds % 3600) // 60)
    duration_secs = int(duration_seconds % 60)
    
    # FFmpeg parancs VMAF számításhoz
    # libvmaf filter használata teljes videóra
    # Automatikus processzormag detektálás és használat
    cpu_count = multiprocessing.cpu_count()
    
    # FFmpeg 8.0+: VMAF és PSNR egy parancsban (feature=name=psnr)
    base_libvmaf_filter = f'libvmaf=n_threads={cpu_count}:feature=name=psnr'
    libvmaf_filter = base_libvmaf_filter
    
    ffmpeg_cmd = [
        FFMPEG_PATH,
        '-i', reference_str,
        '-i', encoded_str,
        '-lavfi', libvmaf_filter,
        '-f', 'null',
        '-'
    ]
    
    # FFmpeg parancs kiírása a logger-be
    if logger:
        logger.write(f"\n{'='*80}\n")
        logger.write(f"🎬 VMAF/PSNR SZÁMÍTÁS PARANCS:\n")
        logger.write(f"{'='*80}\n")
        logger.write(' '.join(ffmpeg_cmd) + '\n')
        logger.write(f"Videó hossza: {duration_hours:02d}:{duration_mins:02d}:{duration_secs:02d}\n")
        logger.write(f"{'='*80}\n\n")
        logger.flush()
    
    try:
        process = subprocess.Popen(
            ffmpeg_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            startupinfo=get_startup_info()
        )
        
        with ACTIVE_PROCESSES_LOCK:
            ACTIVE_PROCESSES.append(process)
        
        # VMAF és PSNR érték kinyerése a stdout-ból
        vmaf_value = None
        psnr_value = None
        full_output = []
        
        try:
            for line in process.stdout:
                if stop_event.is_set():
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                    raise EncodingStopped()
                
                full_output.append(line)
                
                # FFmpeg kimenet kiírása a logger-be
                if logger:
                    logger.write(line)
                    logger.flush()
                
                # VMAF érték keresése a kimenetben
                # libvmaf formátum: "VMAF score: XX.XXXX" vagy "VMAF score = XX.XXXX"
                vmaf_match = re.search(r'VMAF\s+score[:\s=]+\s*([\d.]+)', line, re.IGNORECASE)
                if vmaf_match:
                    try:
                        vmaf_value = float(vmaf_match.group(1))
                    except ValueError:
                        pass
                
                # PSNR érték keresése a kimenetben
                # libvmaf formátum: "PSNR score: XX.XXXX" vagy "PSNR score = XX.XXXX" vagy "PSNR: XX.XXXX"
                psnr_match = re.search(r'PSNR\s+(?:score[:\s=]+|:)\s*([\d.]+)', line, re.IGNORECASE)
                if psnr_match:
                    try:
                        psnr_value = float(psnr_match.group(1))
                    except ValueError:
                        pass
                
                # Progress információ kinyerése - időtartam formátumban
                if progress_callback and ('frame=' in line or 'time=' in line):
                    if 'time=' in line:
                        time_match = re.search(r'time=(\d+):(\d+):(\d+)(?:\.\d+)?', line)
                        if time_match:
                            hours, mins, secs = map(int, time_match.groups())
                            elapsed_total = hours * 3600 + mins * 60 + secs
                            # Korlátozzuk a videó hosszára
                            elapsed_total = min(elapsed_total, duration_seconds) if duration_seconds > 0 else elapsed_total
                            
                            # Időtartam formátum: "HH:MM:SS / HH:MM:SS"
                            progress_hours = int(elapsed_total // 3600)
                            progress_mins = int((elapsed_total % 3600) // 60)
                            progress_secs = int(elapsed_total % 60)
                            
                            progress_callback(f"{progress_hours:02d}:{progress_mins:02d}:{progress_secs:02d} / {duration_hours:02d}:{duration_mins:02d}:{duration_secs:02d}")
        except EncodingStopped:
            raise
        except (OSError, IOError, BrokenPipeError, UnicodeDecodeError) as e:
            if logger:
                logger.write(f"Process output olvasás hiba: {e}\n")
                logger.flush()
            print(f"✗ Process output olvasás hiba: {e}")
        finally:
            # KÖZEPES JAVÍTÁS #9: Process törlése a listából - mindig végrehajtjuk
            with ACTIVE_PROCESSES_LOCK:
                if process in ACTIVE_PROCESSES:
                    ACTIVE_PROCESSES.remove(process)
        
        # KÖZEPES JAVÍTÁS #9: process.wait() áthelyezve a try blokkba (2567. sor), így nincs duplikált cleanup
        if process.returncode != 0:
            error_msg = f"✗ VMAF számítás hiba (return code: {process.returncode})"
            if logger:
                logger.write(error_msg + '\n')
                logger.flush()
            print(error_msg)
            if LIBVMAF_SUPPORTS_PSNR and ("feature" in ''.join(full_output).lower()):
                LIBVMAF_SUPPORTS_PSNR = False
                print("⚠ Libvmaf nem támogatja a feature=name=psnr opciót – PSNR külön futtatásra váltok.")
                return _calculate_full_vmaf_ffmpeg(reference_path, encoded_path, progress_callback, stop_event, logger)
            return None
        
        if vmaf_value is None or psnr_value is None:
            # Próbáljuk meg az összes sorban keresni
            full_output_text = ''.join(full_output)
            if vmaf_value is None:
                vmaf_match = re.search(r'VMAF\s+score[:\s=]+\s*([\d.]+)', full_output_text, re.IGNORECASE)
                if vmaf_match:
                    try:
                        vmaf_value = float(vmaf_match.group(1))
                    except ValueError:
                        pass
            if psnr_value is None:
                psnr_match = re.search(r'PSNR\s+(?:score[:\s=]+|:)\s*([\d.]+)', full_output_text, re.IGNORECASE)
                if psnr_match:
                    try:
                        psnr_value = float(psnr_match.group(1))
                    except ValueError:
                        pass
        
        return (vmaf_value, psnr_value)
        
    except EncodingStopped:
        raise
    except Exception as e:
        error_msg = f"✗ VMAF/PSNR számítás hiba: {e}"
        if logger:
            logger.write(error_msg + '\n')
            logger.flush()
        print(error_msg)
        return (None, None)

def update_video_metadata_vmaf(video_path, vmaf_value, psnr_value=None, logger=None):
    """Update video metadata with calculated VMAF and PSNR values.
    
    Writes the values to the file metadata using FFmpeg.
    
    Args:
        video_path: Path to the video file.
        vmaf_value: Calculated VMAF score.
        psnr_value: Calculated PSNR score (optional).
        logger: Logger instance.
        
    Returns:
        bool: True if successful, False otherwise.
    """
    video_str = None
    temp_path = None
    try:
        video_str = os.fspath(video_path.absolute())
        
        # Metaadatokhoz nyelvfüggetlen (angol) formátumot használunk
        vmaf_str_formatted = format_number_en(vmaf_value, decimals=2) if vmaf_value is not None else None
        psnr_str_formatted = format_number_en(psnr_value, decimals=2) if psnr_value is not None else None

        if logger:
            logger.write(f"\n{'='*80}\n")
            logger.write(f"📝 VMAF/PSNR METADATA FRISSÍTÉS: {video_path.name}\n")
            logger.write(f"{'='*80}\n")
            logger.write(f"Új VMAF érték: {vmaf_str_formatted}\n")
            if psnr_value is not None:
                logger.write(f"Új PSNR érték: {psnr_str_formatted}\n")
            logger.flush()
        
        # Megkeressük a jelenlegi Settings metaadatot
        probe_cmd = [
            FFPROBE_PATH, '-v', 'error',
            '-show_entries', 'format_tags=Settings',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            video_str
        ]
        
        result = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=30, startupinfo=get_startup_info())
        current_settings = result.stdout.strip() if result.stdout else ""
        
        if logger:
            logger.write(f"Jelenlegi Settings: {current_settings}\n")
            logger.flush()
        
        # Új Settings metaadat létrehozása VMAF értékkel
        if current_settings:
            # Ha már van Settings, frissítjük a VMAF részt
            # Formátum: "FFMPEG NVENC - CQ:XX - Preset 7 - Planned VMAF: XX.X"
            # Vagy: "FFMPEG SVT-AV1 - CRF:XX - Preset 2 - Planned VMAF: XX.X"
            # Vagy: "... - Actual VMAF: XX.X" (ha már volt VMAF számítás)
            new_settings = current_settings
            
            # Ha már van "Actual VMAF", azt frissítjük
            # A regex kezeli a tizedesvesszőt és tizedespontot is
            if 'Actual VMAF' in current_settings:
                # Először eltávolítjuk az összes "Actual VMAF: ..." részt (akár több is lehet)
                new_settings = re.sub(
                    r'Actual VMAF:\s*[\d.,]+(?:\s*-\s*)?',
                    '',
                    current_settings
                ).strip()
                # Eltávolítjuk a felesleges " - " részeket
                new_settings = re.sub(r'\s*-\s*-\s*', ' - ', new_settings)
                new_settings = re.sub(r'^\s*-\s*', '', new_settings)
                new_settings = re.sub(r'\s*-\s*$', '', new_settings)
                # Hozzáadjuk az új "Actual VMAF" értéket
                if new_settings:
                    new_settings = f"{new_settings} - Actual VMAF: {vmaf_str_formatted}"
                else:
                    new_settings = f"Actual VMAF: {vmaf_str_formatted}"
            # Ha van "Planned VMAF", azt cseréljük le "Actual VMAF"-ra
            elif 'Planned VMAF' in current_settings:
                new_settings = re.sub(
                    r'Planned VMAF:\s*[\d.,]+',
                    f'Actual VMAF: {vmaf_str_formatted}',
                    current_settings
                )
            # Ha egyáltalán nem volt VMAF információ, hozzáadjuk
            else:
                new_settings = f"{current_settings} - Actual VMAF: {vmaf_str_formatted}"
            
            # PSNR hozzáadása/frissítése
            # A regex kezeli a tizedesvesszőt és tizedespontot is
            if psnr_value is not None:
                if 'PSNR:' in new_settings:
                    new_settings = re.sub(
                        r'PSNR:\s*[\d.,]+',
                        f'PSNR: {psnr_str_formatted}',
                        new_settings
                    )
                else:
                    new_settings = f"{new_settings} - PSNR: {psnr_str_formatted}"
        else:
            # Ha nincs Settings metaadat, létrehozzuk
            if psnr_value is not None:
                new_settings = f"Actual VMAF: {vmaf_str_formatted} - PSNR: {psnr_str_formatted}"
            else:
                new_settings = f"Actual VMAF: {vmaf_str_formatted}"
        
        # FFmpeg parancs metaadat frissítéshez (copy minden streamet, csak metaadatot módosítjuk)
        # Temp fájl az eredeti kiterjesztéssel, hogy az FFmpeg felismerje a formátumot
        # Kezeljük a több kiterjesztésű fájlokat is (pl. teszt.av1.mkv -> teszt.av1.tmp.mkv)
        file_name = video_path.name
        # Keresünk egy pontot, ahol be tudjuk szúrni a .tmp-ot a végső kiterjesztés elé
        if '.' in file_name:
            # Megkeressük az utolsó pontot (a végső kiterjesztés)
            last_dot_index = file_name.rfind('.')
            temp_name = file_name[:last_dot_index] + '.tmp' + file_name[last_dot_index:]
        else:
            # Ha nincs kiterjesztés, csak hozzáfűzzük a .tmp-ot
            temp_name = file_name + '.tmp'
        temp_path = Path(video_path.parent / temp_name)
        ffmpeg_cmd = [
            FFMPEG_PATH, '-i', video_str,
            '-map', '0',            # Minden stream megőrzése (videó, hang, felirat, melléklet)
            '-c', 'copy',           # Copy minden streamet
            '-map_metadata', '0',   # Meglévő metaadatok megtartása
            '-metadata', f'Settings={new_settings}',
            '-y',  # Overwrite
            os.fspath(temp_path)
        ]
        
        if logger:
            logger.write(f"Új Settings: {new_settings}\n")
            logger.write(f"FFmpeg parancs: {' '.join(ffmpeg_cmd)}\n")
            logger.flush()
        
        result = subprocess.run(ffmpeg_cmd, capture_output=True, text=True, timeout=300, startupinfo=get_startup_info())
        
        if result.returncode == 0:
            # Sikeres, átnevezzük a fájlt
            if temp_path.exists():
                temp_path.replace(video_path)
                if logger:
                    logger.write(f"✓ Metadata frissítés sikeres: {video_path.name}\n")
                    logger.write(f"{'='*80}\n\n")
                    logger.flush()
                return True
            else:
                if logger:
                    logger.write(f"✗ Hiba: Temp fájl nem jött létre: {temp_path}\n")
                    logger.write(f"{'='*80}\n\n")
                    logger.flush()
        else:
            if logger:
                logger.write(f"✗ FFmpeg hiba (returncode: {result.returncode}):\n")
                if result.stderr:
                    logger.write(f"STDERR: {result.stderr}\n")
                if result.stdout:
                    logger.write(f"STDOUT: {result.stdout}\n")
                logger.write(f"{'='*80}\n\n")
                logger.flush()
        
        return False
        
    except Exception as e:
        error_msg = f"✗ Metaadat frissítés hiba: {e}"
        if logger:
            logger.write(f"\n{error_msg}\n")
            import traceback
            logger.write(f"Traceback:\n{traceback.format_exc()}\n")
            logger.write(f"{'='*80}\n\n")
            logger.flush()
        print(error_msg)
        # Temp fájl törlése hiba esetén
        try:
            if temp_path and temp_path.exists():
                temp_path.unlink()
        except (OSError, PermissionError):
            pass
        return False

def get_video_info(video_path):
    """Retrieve basic video information using FFprobe.
    
    Args:
        video_path: Path to the video file (Path or str).
        
    Returns:
        tuple: (duration, fps) where:
            - duration: Video duration in seconds (float)
            - fps: Frame rate (float)
            Returns (None, None) on error.
    """
    try:
        # Duration lekérdezése
        cmd_duration = [
            FFPROBE_PATH, '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            os.fspath(video_path)
        ]
        result = subprocess.run(cmd_duration, capture_output=True, text=True, check=True, timeout=30, startupinfo=get_startup_info())
        duration = float(result.stdout.strip())
        
        # FPS lekérdezése
        cmd_fps = [
            FFPROBE_PATH, '-v', 'error',
            '-select_streams', 'v:0',
            '-show_entries', 'stream=r_frame_rate',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            os.fspath(video_path)
        ]
        try:
            result_fps = subprocess.run(cmd_fps, capture_output=True, text=True, check=True, timeout=30, startupinfo=get_startup_info())
            fps_str = result_fps.stdout.strip()
            if fps_str and '/' in fps_str:
                num, den = map(int, fps_str.split('/'))
                fps = num / den if den > 0 else 25.0
            else:
                fps = 25.0
        except (ValueError, ZeroDivisionError, AttributeError):
            fps = 25.0
        
        return duration, fps
    except Exception as e:
        print(f"✗ FFprobe hiba: {e}")
        return None, None

def get_video_resolution(video_path):
    """Get video resolution (width, height) using FFprobe.
    
    Args:
        video_path: Path to the video file.
        
    Returns:
        tuple: (width, height) or (None, None) on error.
    """
    try:
        cmd = [
            FFPROBE_PATH, '-v', 'error',
            '-select_streams', 'v:0',
            '-show_entries', 'stream=width,height',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            os.fspath(video_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=30, startupinfo=get_startup_info())
        lines = result.stdout.strip().split('\n')
        if len(lines) >= 2:
            width = int(lines[0].strip())
            height = int(lines[1].strip())
            return width, height
        return None, None
    except (ValueError, subprocess.SubprocessError, OSError, AttributeError):
        return None, None

def get_video_frame_count(video_path):
    """Get video frame count using FFprobe.
    
    Args:
        video_path: Path to the video file.
        
    Returns:
        int: Frame count or None on error.
    """
    if not video_path.exists():
        return None

    cmd_frames = [
        FFPROBE_PATH, '-v', 'error',
        '-select_streams', 'v:0',
        '-show_entries', 'stream=nb_frames',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        os.fspath(video_path)
    ]
    try:
        result = subprocess.run(cmd_frames, capture_output=True, text=True, check=True, timeout=30, startupinfo=get_startup_info())
        frames_str = result.stdout.strip()
        if frames_str:
            return int(frames_str)
    except (ValueError, subprocess.SubprocessError, OSError, AttributeError):
        pass

    duration, fps = get_video_info(video_path)
    if duration and fps:
        try:
            return int(duration * fps)
        except (ValueError, TypeError):
            return None
    return None

def get_output_file_info(output_path):
    """Get information about the output file using FFprobe.
    
    Args:
        output_path: Path to the output file.
        
    Returns:
        tuple: (cq_crf, vmaf, psnr, frame_count, file_size, modified_date, encoder_type, should_delete)
        
        Returns (None, None, None, None, None, None, None, False, None) if the file does not exist or an error occurs.
    """
    if not output_path or not output_path.exists():
        return None, None, None, None, None, None, None, False, None

    try:
        duration_seconds = None
        try:
            duration_seconds, _ = get_video_info(output_path)
        except Exception:
            duration_seconds = None

        # Settings metaadat lekérdezése (CQ/CRF és VMAF)
        probe_cmd = [
            FFPROBE_PATH, '-v', 'error',
            '-show_entries', 'format_tags=Settings',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            os.fspath(output_path.absolute())
        ]
        result = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=30, startupinfo=get_startup_info())
        settings_str = result.stdout.strip() if result.stdout else ""
        
        cq_crf = None
        vmaf = None
        psnr = None
        encoder_type = None
        
        if settings_str:
            # Encoder típus kinyerése (NVENC vagy SVT-AV1)
            # Először NVENC-et keresünk (mert lehet, hogy mindkettő benne van)
            if 'NVENC' in settings_str.upper() or 'CQ:' in settings_str:
                encoder_type = 'nvenc'
            elif 'SVT-AV1' in settings_str.upper() or 'SVT' in settings_str.upper() or 'CRF:' in settings_str:
                encoder_type = 'svt-av1'
            
            # CQ/CRF érték kinyerése
            cq_match = re.search(r'CQ:(\d+)', settings_str)
            crf_match = re.search(r'CRF:(\d+)', settings_str)
            if cq_match:
                cq_crf = int(cq_match.group(1))
            elif crf_match:
                cq_crf = int(crf_match.group(1))
            
            # VMAF érték kinyerése (Actual VMAF vagy Planned VMAF)
            vmaf_match = re.search(r'(?:Actual|Planned)\s+VMAF:\s*([\d.]+)', settings_str)
            if vmaf_match:
                vmaf = float(vmaf_match.group(1))
            
            # PSNR érték kinyerése
            psnr_match = re.search(r'PSNR:\s*([\d.]+)', settings_str)
            if psnr_match:
                psnr = float(psnr_match.group(1))
        
        # Frame szám lekérdezése
        cmd_frames = [
            FFPROBE_PATH, '-v', 'error',
            '-select_streams', 'v:0',
            '-show_entries', 'stream=nb_frames',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            os.fspath(output_path.absolute())
        ]
        frame_count = None
        try:
            result_frames = subprocess.run(cmd_frames, capture_output=True, text=True, check=True, timeout=30, startupinfo=get_startup_info())
            frames_str = result_frames.stdout.strip()
            if frames_str:
                frame_count = int(frames_str)
        except (ValueError, TypeError, AttributeError):
            # Ha nb_frames nem elérhető, próbáljuk duration és fps alapján számolni
            try:
                duration, fps = get_video_info(output_path)
                if duration and fps:
                    frame_count = int(duration * fps)
            except (ValueError, TypeError, AttributeError, Exception):
                pass
        
        # Fájlméret
        if not output_path.exists():
            return None, None, None, None, None, None, None, False, duration_seconds
        file_size = output_path.stat().st_size

        # Utolsó módosítási dátum
        modified_timestamp = output_path.stat().st_mtime
        modified_date = datetime.fromtimestamp(modified_timestamp).strftime('%Y-%m-%d %H:%M:%S')

        return cq_crf, vmaf, psnr, frame_count, file_size, modified_date, encoder_type, False, duration_seconds

    except Exception as e:
        print(f"✗ Célfájl FFprobe hiba ({output_path.name}): {e}")
        return None, None, None, None, None, None, None, False, duration_seconds


def frames_significantly_different(source_frames, output_frames):
    """Eldönti, hogy a kimeneti frame szám jelentősen eltér-e a forrástól."""
    if source_frames is None or output_frames is None:
        return False
    allowed_diff = max(FRAME_MISMATCH_MIN_DIFF, int(source_frames * FRAME_MISMATCH_RATIO))
    return abs(source_frames - output_frames) > allowed_diff

def normalize_audio_lang(lang_string):
    """Normalizálja a hangsáv nyelv kódját az összehasonlításhoz"""
    if not lang_string:
        return 'unknown'
    lang_clean = lang_string.strip().lower()
    # Ha van kötőjel, csak az első részt vesszük (pl. "hun-HUN" -> "hun")
    if '-' in lang_clean:
        lang_clean = lang_clean.split('-')[0]
    
    # Ha már 2 karakteres kód, ellenőrizzük, hogy van-e a LANGUAGE_MAP-ban
    if len(lang_clean) == 2:
        if lang_clean in LANGUAGE_MAP:
            return lang_clean
        return lang_clean
    
    # Ha 3 karakteres vagy hosszabb, keresünk fordított mapping-et a LANGUAGE_MAP-ban
    # LANGUAGE_MAP formátum: 'hu': 'hun', 'en': 'eng', stb.
    # Keresünk olyan kulcsot, ahol az érték megegyezik a lang_clean-nel
    for key, value in LANGUAGE_MAP.items():
        if value == lang_clean:
            # Ha a kulcs 2 karakteres, azt adjuk vissza
            if len(key) == 2:
                return key
            # Ha a kulcs 3 karakteres, az első 2 karaktert adjuk vissza
            if len(key) == 3:
                return key[:2]
        # Ha a kulcs megegyezik a lang_clean-nel és 2 karakteres
        if key == lang_clean and len(key) == 2:
            return key
    
    # Ha 3 karakteres nyelv kód (pl. "hun", "eng"), akkor az első 2 karaktert használjuk
    if len(lang_clean) == 3:
        return lang_clean[:2]
    
    return lang_clean

def get_audio_streams_info(video_path):
    """Analyze audio streams in the video.
    
    Identifies the default language and counts 5.1 and 2.0 streams per language.
    
    Args:
        video_path: Path to the video file.
        
    Returns:
        tuple: (default_lang, lang_51_count, lang_20_count)
    """
    """Visszaadja a hangsávok információit: default nyelv, 5.1 és 2.0 hangsávok száma nyelv szerint"""
    try:
        # Összes hangsáv információ lekérdezése (disposition is kell a default hangsávhoz)
        cmd = [
            FFPROBE_PATH, '-v', 'error',
            '-select_streams', 'a',
            '-show_entries', 'stream=index,channels,disposition',
            '-show_entries', 'stream_tags=language,title',
            '-of', 'json',
            os.fspath(video_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=30, startupinfo=get_startup_info())
        data = json.loads(result.stdout)
        
        if 'streams' not in data or len(data['streams']) == 0:
            return None, {}, {}
        
        default_lang = None
        lang_51_count = {}  # Normalizált nyelv -> 5.1 hangsávok száma
        lang_20_count = {}  # Normalizált nyelv -> 2.0 hangsávok száma
        all_streams = []  # Összes hangsáv információ tárolása
        
        # Első körben összegyűjtjük az összes hangsáv információt
        for stream in data['streams']:
            channels = stream.get('channels', 0)
            tags = stream.get('tags', {})
            lang = tags.get('language', '') or tags.get('lang', '') or ''
            disposition = stream.get('disposition', {})
            # A disposition objektum tartalmazza a 'default' mezőt (0 vagy 1)
            is_default = disposition.get('default', 0) == 1 if isinstance(disposition, dict) else False
            
            lang_normalized = normalize_audio_lang(lang) if lang else 'unknown'
            
            all_streams.append({
                'lang': lang_normalized,
                'channels': channels,
                'is_default': is_default
            })
            
            # 5.1 hangsávok számlálása nyelv szerint (6 csatorna)
            if channels == 6:
                lang_51_count[lang_normalized] = lang_51_count.get(lang_normalized, 0) + 1
            # 2.0 hangsávok számlálása nyelv szerint (2 csatorna)
            elif channels == 2:
                lang_20_count[lang_normalized] = lang_20_count.get(lang_normalized, 0) + 1
        
        # Alapértelmezett nyelv meghatározása prioritás szerint:
        # 1. Explicit default hangsáv (disposition:default=1)
        for stream_info in all_streams:
            if stream_info['is_default']:
                default_lang = stream_info['lang']
                break
        
        # 2. Ha nincs explicit default, az applikáció nyelvének megfelelő hangsáv
        if default_lang is None or default_lang == 'unknown':
            app_lang = CURRENT_LANGUAGE  # 'hu' vagy 'en'
            # Keresünk olyan hangsávot, amelynek a normalizált nyelve megegyezik az applikáció nyelvével
            for stream_info in all_streams:
                if stream_info['lang'] == app_lang:
                    default_lang = stream_info['lang']
                    break
        
        # 3. Ha még mindig nincs, az első hangsávot használjuk
        if default_lang is None or default_lang == 'unknown':
            if len(all_streams) > 0:
                default_lang = all_streams[0]['lang']
            else:
                default_lang = 'unknown'
        
        return default_lang, lang_51_count, lang_20_count
    except Exception as e:
        print(f"✗ FFprobe hangsáv info hiba: {e}")
        return None, {}, {}

def get_audio_stream_details(video_path):
    """Részletes hangsáv-információk (menühöz, eltávolításhoz)."""
    try:
        # Első lekérdezés: stream információk
        cmd = [
            FFPROBE_PATH, '-v', 'error',
            '-show_entries', 'stream=index,codec_name,codec_type,channels,channel_layout,bit_rate',
            '-show_entries', 'stream_tags=language,title',
            '-show_entries', 'format=duration',
            '-of', 'json',
            os.fspath(video_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=30, startupinfo=get_startup_info())
        data = json.loads(result.stdout) if result.stdout else {}
        streams = data.get('streams', [])
        format_info = data.get('format', {})
        duration = float(format_info.get('duration', 0)) if format_info.get('duration') else 0
        
        details = []
        audio_index = 0
        for stream in streams:
            if stream.get('codec_type') != 'audio':
                continue
            codec = (stream.get('codec_name') or '').upper() or 'UNKNOWN'
            channels = stream.get('channels', 0)
            channel_layout = stream.get('channel_layout') or ''
            tags = stream.get('tags', {})
            language = tags.get('language') or tags.get('lang') or 'unknown'
            title = tags.get('title', '')
            lang_normalized = normalize_audio_lang(language)
            display_lang = language or lang_normalized or 'unknown'
            
            # Hangsáv méret számítása bitrate és hossz alapján
            bit_rate = stream.get('bit_rate')
            audio_size_mb = None
            if bit_rate and duration:
                try:
                    bit_rate_int = int(bit_rate)
                    # bit_rate bps-ben van, duration másodpercben
                    audio_size_bytes = (bit_rate_int * duration) / 8
                    audio_size_mb = audio_size_bytes / (1024 * 1024)
                except (ValueError, TypeError):
                    pass
            
            if channels == 6:
                channel_label = '5.1'
            elif channels == 2:
                channel_label = '2.0'
            elif channels and isinstance(channels, int):
                channel_label = f'{channels} ch'
            elif channel_layout:
                channel_label = channel_layout
            else:
                channel_label = '?'
            description_parts = [display_lang.upper(), channel_label, codec]
            description = ' | '.join(part for part in description_parts if part)
            if title:
                description = f"{description} - {title}"
            if audio_size_mb is not None:
                # Lokalizált formátum: magyar = tizedesvessző, angol = tizedespont
                size_str = format_localized_number(audio_size_mb, decimals=2)
                description = f"{description} ({size_str} MB)"
            details.append({
                'ffmpeg_audio_index': audio_index,
                'language': display_lang,
                'language_normalized': lang_normalized,
                'channels': channels,
                'channel_layout': channel_layout,
                'codec': codec,
                'title': title,
                'description': description,
                'size_mb': audio_size_mb
            })
            audio_index += 1
        return details
    except Exception as e:
        print(f"✗ Audio stream info hiba: {e}")
        return []

def get_51_audio_stream_index(video_path, default_lang):
    """Get detailed information about audio streams.
    
    Args:
        video_path: Path to the video file.
        
    Returns:
        list: List of audio stream details.
    """
    try:
        cmd = [
            FFPROBE_PATH, '-v', 'error',
            '-select_streams', 'a',
            '-show_entries', 'stream=index,channels',
            '-show_entries', 'stream_tags=language,title',
            '-of', 'json',
            os.fspath(video_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=30, startupinfo=get_startup_info())
        data = json.loads(result.stdout)
        
        if 'streams' not in data or len(data['streams']) == 0:
            return None
        
        # Hangsávok sorrendjében számoljuk (0, 1, 2, ...)
        audio_stream_index = 0
        for stream in data['streams']:
            channels = stream.get('channels', 0)
            tags = stream.get('tags', {})
            lang = tags.get('language', '') or tags.get('lang', '') or ''
            
            # Normalizáljuk a nyelv kódot az összehasonlításhoz
            lang_normalized = normalize_audio_lang(lang)
            
            # Ha 5.1 hangsáv (6 csatorna) és ugyanaz a normalizált nyelv, mint az alapértelmezett
            if channels == 6 and lang_normalized == default_lang:
                return audio_stream_index
            
            # Következő hangsáv index
            audio_stream_index += 1
        
        return None
    except Exception as e:
        print(f"✗ 5.1 hangsáv index keresés hiba: {e}")
        return None

def check_audio_compression_needed(video_path):
    """Check if audio dynamic range compression is needed.
    
    Args:
        video_path: Path to the video file.
        
    Returns:
        bool: True if compression is recommended.
    """
    try:
        default_lang, lang_51_count, lang_20_count = get_audio_streams_info(video_path)
        
        if default_lang is None:
            return False
        
        # Ha az alapértelmezett nyelvhez nincs 5.1 hangsáv, nem kell kompresszió
        if lang_51_count.get(default_lang, 0) == 0:
            return False
        
        # Ha az alapértelmezett nyelvhez van 2.0 hangsáv, nem kell kompresszió
        if lang_20_count.get(default_lang, 0) > 0:
            return False
        
        # Ha az alapértelmezett nyelvhez van 5.1 hangsáv és nincs 2.0, akkor kell kompresszió
        return True
    except Exception as e:
        print(f"✗ Hangdinamika kompresszió ellenőrzés hiba: {e}")
        return False

def build_audio_conversion_filter(method='fast'):
    """Build FFmpeg filter chain for 5.1 to 2.0 audio conversion.
    
    Args:
        method: 'fast' (simple mix) or 'high_quality' (compand/normalization).
        
    Returns:
        str: FFmpeg filter string.
    """
    method_key = (method or 'fast').lower()
    if method_key == 'dialogue':
        return 'pan=stereo|FL<FL+0.5*FC+BL+0.6*SL|FR<FR+0.5*FC+BR+0.6*SR,dynaudnorm=f=250:g=8:m=7.0,alimiter=limit=0.98:level=disabled'
    return 'pan=stereo|FL<FL+0.5*FC+BL+0.6*SL|FR<FR+0.5*FC+BR+0.6*SR,acompressor=threshold=-18dB:ratio=4:attack=10:release=200:makeup=6:knee=2,alimiter=limit=0.9:level=disabled'

def get_audio_conversion_title(method):
    """Metaadat cím generálása a kiválasztott konverziós módszerhez."""
    method_key = (method or 'fast').lower()
    if method_key == 'dialogue':
        return t('audio_convert_title_dialogue')
    return t('audio_convert_title_fast')

def find_vdub2_path():
    """Find VirtualDub2 executable in PATH or common locations.
    
    Returns:
        Path: Path to vdub2.exe/vdub64.exe or None.
    """
    """VirtualDub2 elérési út meghatározása felhasználói beállítás vagy automatikus keresés alapján."""
    if VDUB2_PATH:
        vdub_path = VDUB2_PATH if isinstance(VDUB2_PATH, Path) else Path(VDUB2_PATH)
        if vdub_path.exists():
            return vdub_path
    
    detected = find_virtualdub()
    if detected:
        detected_path = Path(detected)
        if detected_path.exists():
            return detected_path
    
    possible_paths = [
        Path("vdub2.exe"),
        Path("vdub64.exe"),
        Path("C:/VirtualDub2_v2.4/vdub2.exe"),
        Path("D:/VirtualDub2_v2.4/vdub2.exe"),
        Path("C:/VirtualDub2/vdub2.exe"),
        Path("D:/VirtualDub2/vdub2.exe"),
        Path("C:/Program Files/VirtualDub2/vdub2.exe"),
        Path("C:/Program Files (x86)/VirtualDub2/vdub2.exe"),
    ]
    for path in possible_paths:
        if path.exists():
            return path
    
    vdub2_path = shutil.which("vdub2.exe") or shutil.which("vdub64.exe")
    if vdub2_path:
        return Path(vdub2_path)
    
    return None

def extract_frames_with_vdub2(video_path, output_dir, num_frames=5, stop_event=None, use_percentage_positions=False):
    """Export sample frames from video using VirtualDub2.
    
    Args:
        video_path: Path to the video file.
        output_dir: Directory to save frames.
        num_frames: Number of frames to export (if use_percentage_positions is False).
        stop_event: Event to stop the process.
        use_percentage_positions: If True, exports frames at 30%, 50%, 70%, and end.
        
    Returns:
        list: List of paths to extracted frames.
    """
    vdub2_path = find_vdub2_path()
    if not vdub2_path:
        print("  ⚠ VirtualDub2 nem található")
        return []
    
    duration, _ = get_video_info(video_path)
    if not duration or duration < 1:
        print("  ⚠ Nem sikerült a videó hosszát meghatározni")
        return []
    
    if use_percentage_positions:
        # 30%, 50%, 70% és vége (4db frame)
        frame_times = [
            duration * 0.30,  # 30%
            duration * 0.50,  # 50%
            duration * 0.70,  # 70%
            duration - 0.1    # Vége (0.1 másodperccel a vég előtt, hogy biztosan legyen frame)
        ]
        print(f"  📹 VirtualDub2 frame export: 4 képkocka (30%, 50%, 70%, vége)")
    else:
        # Régi módszer: random pozíciók
        frame_times = sorted([random.uniform(0, duration) for _ in range(num_frames)])
        print(f"  📹 VirtualDub2 frame export: {num_frames} képkocka")
    
    duration_str = format_localized_number(duration, decimals=1)
    print(f"  Videó hossza: {duration_str}s")
    print(f"  VirtualDub2: {vdub2_path}")
    
    if stop_event is None:
        stop_event = STOP_EVENT

    source_frame_count = None
    encoded_frame_count = None
    frame_count_warning = False
    last_frame_ok = True

    if stop_event.is_set():
        raise EncodingStopped()

    extracted_frames = []
    
    for i, t in enumerate(frame_times, start=1):
        if stop_event.is_set():
            raise EncodingStopped()
        frame_filename = "frame0000.png"
        frame_path = output_dir / frame_filename
        
        time_str = format_localized_number(t, decimals=2)
        print(f"    Frame #{i} @ {time_str}s...")
        
        # VirtualDub2 script létrehozása
        script_path = output_dir / f"script_{i}.vdscript"
        frame_index = int(t * 25)  # 25 fps feltételezése
        
        # Útvonalak dupla backslash-sel (Windows formátum)
        video_path_str = str(video_path.absolute()).replace("\\", "\\\\")
        output_dir_str = str(output_dir.absolute()).replace("\\", "\\\\")
        
        script_content = f"""VirtualDub.Open("{video_path_str}");
VirtualDub.subset.Clear();
VirtualDub.subset.AddRange({frame_index}, 1);
VirtualDub.SaveImageSequence("{output_dir_str}\\\\frame", ".png", 4, 3);
VirtualDub.Close();
"""
        
        try:
            if stop_event.is_set():
                raise EncodingStopped()
            with open(script_path, 'w', encoding='utf-8') as f:
                f.write(script_content)
        except (OSError, PermissionError, IOError) as e:
            print(f"      ✗ Script írási hiba: {e}")
            continue
        
        # Script tartalmának kiírása konzolra
        print(f"\n{'='*80}")
        print("VDUB2 SCRIPT TARTALOM:")
        print(f"{'='*80}")
        print(script_content)
        print(f"{'='*80}\n")
        
        cmd = [
            os.fspath(vdub2_path),
            "/s", os.fspath(script_path),
            "/x"
        ]
        
        print(f"\n{'='*80}")
        print("VDUB2 PARANCS:")
        print(f"{'='*80}")
        print(" ".join(cmd))
        print(f"{'='*80}\n")
        
        try:
            if stop_event.is_set():
                raise EncodingStopped()
            
            full_output = []
            # Context manager használata a process kezeléshez
            with managed_subprocess(cmd, cwd=output_dir, stop_event=stop_event, timeout=180) as process:
                try:
                    for line in process.stdout:
                        if stop_event and stop_event.is_set():
                            raise EncodingStopped()
                        print(line.rstrip())
                        full_output.append(line)
                except EncodingStopped:
                    raise
                except (OSError, IOError, BrokenPipeError, UnicodeDecodeError) as e:
                    print(f"VirtualDub2 output olvasás hiba: {e}")
                
                # Várakozás a process befejezésére
                try:
                    process.wait(timeout=180)
                except subprocess.TimeoutExpired:
                    # A context manager finally blokkja kezeli a cleanup-ot
                    raise
            
            full_output_text = ''.join(full_output)
            
            if stop_event.is_set():
                raise EncodingStopped()
            
            print(f"\n{'='*80}")
            print(f"VDUB2 BEFEJEZVE - Return code: {process.returncode}")
            print(f"{'='*80}\n")
            
            # "unexpected end of stream" detektálása
            if "unexpected end of stream" in full_output_text.lower():
                print(f"      ✗ 'unexpected end of stream' - videó lejátszható-e?")
                return None  # Speciális jelzés az újrakódoláshoz
            
            # Frame keresése és átnevezése
            if frame_path.exists():
                # Átnevezés egyedi névre
                final_frame_path = output_dir / f"frame_{i:04d}.png"
                frame_path.rename(final_frame_path)
                extracted_frames.append(final_frame_path)
                print(f"      ✓ Frame mentve: {final_frame_path.name}")
            else:
                print(f"      ✗ Nem készült frame")
                
        except EncodingStopped:
            raise
        except subprocess.TimeoutExpired:
            print(f"      ✗ Timeout")
        except (OSError, subprocess.SubprocessError, ValueError, AttributeError) as e:
            print(f"      ✗ Hiba: {e}")
        except Exception as e:
            # Váratlan hibák esetén is logoljuk, de ne akadjon el
            print(f"      ✗ Váratlan hiba: {type(e).__name__}: {e}")
        finally:
            # Script törlése
            try:
                script_path.unlink()
            except (OSError, PermissionError, FileNotFoundError):
                pass
    
    print(f"  Összesen {len(extracted_frames)}/{num_frames} frame exportálva")
    
    debug_pause(
        f"VirtualDub2 frame export kész: {len(extracted_frames)} frame",
        "Frame tartalom ellenőrzés (fekete/üres detektálás)",
        f"Frame-ek: {output_dir}"
    )
    
    return extracted_frames

def is_frame_black_or_empty(frame_path, max_mean_brightness=MAX_MEAN_BRIGHTNESS, min_std_dev=MIN_STD_DEV):
    try:
        with Image.open(frame_path) as img:
            img_array = np.array(img)
            
            file_size = frame_path.stat().st_size
            # KISEBB JAVÍTÁS #16: Konstans használata
            if file_size < MIN_FRAME_FILE_SIZE:
                print(f"      ⚠ Túl kicsi: {file_size} byte")
                return True
            
            if len(img_array.shape) == 3:
                img_gray = np.dot(img_array[...,:3], [0.299, 0.587, 0.114])
            else:
                img_gray = img_array
            
            mean_brightness = np.mean(img_gray)
            std_dev = np.std(img_gray)
            
            brightness_str = format_localized_number(mean_brightness, decimals=1)
            stddev_str = format_localized_number(std_dev, decimals=1)
            print(f"      Fényerő: {brightness_str}, Szórás: {stddev_str}")
            
            is_black = mean_brightness < max_mean_brightness and std_dev < min_std_dev
            
            if is_black:
                print(f"      ✗ Fekete/üres")
                return True
            else:
                print(f"      ✓ Valódi tartalom")
                return False
    except Exception as e:
        print(f"      ✗ Hiba: {e}")
        return True

def _write_vdub_script(script_path, video_path, start_frame, frame_count, output_prefix):
    video_path_str = str(video_path.absolute()).replace("\\", "\\\\")
    output_prefix_str = str(output_prefix).replace("\\", "\\\\")
    script_content = (
        f'VirtualDub.Open("{video_path_str}");\n'
        "VirtualDub.subset.Clear();\n"
        f"VirtualDub.subset.AddRange({start_frame}, {frame_count});\n"
        f'VirtualDub.SaveImageSequence("{output_prefix_str}", ".png", 4, 3);\n'
        "VirtualDub.Close();\n"
    )
    script_path.write_text(script_content, encoding='utf-8')

def export_specific_frame_with_vdub2(video_path, frame_index, output_path, stop_event=None):
    """Exportál egy konkrét frame-et VirtualDub2 segítségével."""
    vdub2_path = find_vdub2_path()
    if not vdub2_path:
        print("    ✗ VirtualDub2 nem található az utolsó frame exporthoz")
        return False

    if frame_index is None or frame_index < 0:
        print("    ✗ Érvénytelen frame index VirtualDub2 exporthoz")
        return False

    temp_dir = Path(tempfile.mkdtemp())
    try:
        if stop_event is not None and stop_event.is_set():
            raise EncodingStopped()

        script_path = temp_dir / "single_frame.vdscript"
        output_prefix = temp_dir / "frame_last"
        _write_vdub_script(script_path, video_path, frame_index, 1, output_prefix)

        # Script tartalmának ellenőrzése (debug)
        try:
            script_content = script_path.read_text(encoding='utf-8')
            print(f"    VirtualDub2 utolsó frame export ({frame_index}. frame)...")
            print(f"      Script: {script_path}")
            print(f"      Script tartalom:\n        " + "\n        ".join(script_content.strip().splitlines()))
        except Exception as e:
            print(f"    ⚠ Script olvasási hiba: {e}")

        cmd = [
            os.fspath(vdub2_path),
            "/s", os.fspath(script_path),
            "/x"
        ]
        print(f"      Parancs: {' '.join(str(part) for part in cmd)}")
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            startupinfo=get_startup_info(),
            timeout=300
        )
        if result.stdout:
            print("      VDUB2 STDOUT:")
            print("        " + "\n        ".join(result.stdout.strip().splitlines()))
        if result.stderr:
            print("      VDUB2 STDERR:")
            print("        " + "\n        ".join(result.stderr.strip().splitlines()))
        if result.returncode != 0:
            print(f"    ✗ VirtualDub2 hiba ({result.returncode}): {result.stderr.strip() or result.stdout.strip()}")
            return False

        # VirtualDub2 általában "frame_last0000.png" néven hozza létre (aláhúzás nélkül)
        generated_frame = temp_dir / "frame_last0000.png"
        if not generated_frame.exists():
            # Fallback: próbáljuk a "frame_last_0000.png" nevet is (aláhúzással)
            generated_frame = temp_dir / "frame_last_0000.png"
            if not generated_frame.exists():
                # Keresünk minden PNG fájlt a temp könyvtárban
                png_files = list(temp_dir.glob("*.png"))
                if png_files:
                    # Ha találtunk PNG fájlt, használjuk azt
                    generated_frame = png_files[0]
                    print(f"      ⚠ VirtualDub2 más néven hozta létre a fájlt: {generated_frame.name}")
                else:
                    # Ha nem találtunk PNG fájlt, kiírjuk a könyvtár tartalmát
                    try:
                        contents = os.listdir(temp_dir)
                        print(f"      ⚠ VirtualDub2 output könyvtár tartalma: {contents}")
                    except OSError:
                        pass
                    print("    ✗ VirtualDub2 nem hozta létre az utolsó frame képet")
                    return False

        shutil.copy2(generated_frame, output_path)
        return True
    except EncodingStopped:
        raise
    except Exception as e:
        print(f"    ✗ VirtualDub2 utolsó frame export hiba: {e}")
        return False
    finally:
        try:
            shutil.rmtree(temp_dir)
        except OSError:
            pass

def export_last_frame_with_vdub2(video_path, output_path, frame_count_hint=None, stop_event=None):
    """Megpróbálja VirtualDub2-vel kinyerni az utolsó frame-et."""
    frame_count = frame_count_hint
    if frame_count is None:
        try:
            frame_count = get_video_frame_count(video_path)
        except Exception:
            frame_count = None

    if not frame_count or frame_count <= 0:
        print("    ✗ VirtualDub2: nem sikerült meghatározni a frame számot")
        return False

    last_frame_index = max(frame_count - 1, 0)
    return export_specific_frame_with_vdub2(video_path, last_frame_index, output_path, stop_event=stop_event)

def validate_encoded_video_vlc(video_path, encoder='av1_nvenc', stop_event=None, source_path=None):
    """Validate encoded video using various checks (VLC-like validation).
    
    Checks for:
    - File existence and size
    - Duration match with source (if provided)
    - Frame count match (if source provided)
    - Black/empty frames in sample export
    - Last frame readability
    
    Args:
        video_path: Path to the encoded video.
        encoder: Encoder name used.
        stop_event: Event to stop validation.
        source_path: Path to source video (optional, for comparison).
        
    Returns:
        bool: True if validation passes, False otherwise.
    """
    print(f"\n{'='*60}")
    print(f"Videó validáció (VirtualDub2 frame): {video_path.name}")
    print(f"Encoder: {encoder}")
    print(f"{'='*60}")
    
    if stop_event is None:
        stop_event = STOP_EVENT

    if stop_event.is_set():
        raise EncodingStopped()
    
    # KRITIKUS JAVÍTÁS: Változók inicializálása
    source_frame_count = None
    encoded_frame_count = None
    is_valid = True
    success_rate = 0.0
    min_success_rate = 0.4
    frame_count_warning = False
    
    file_size = video_path.stat().st_size
    file_size_mb = file_size / (1024**2)
    print(f"\n1. Fájlméret: {format_localized_number(file_size_mb, decimals=2)} MB")
    
    # KISEBB JAVÍTÁS #16: Konstans használata
    if file_size < MIN_FILE_SIZE_BYTES:
        print("  ✗ Fájl túl kicsi!")
        return False
    
    print("\n2. VirtualDub2 frame export és tartalom ellenőrzés...")
    
    temp_dir = Path(tempfile.mkdtemp())
    try:
        # 30%, 50%, 70% és vége (4db frame összesen)
        print(f"   {encoder}: 4 frame teszt (30%, 50%, 70%, vége)")
        
        extracted_frames = extract_frames_with_vdub2(video_path, temp_dir, num_frames=4, stop_event=stop_event, use_percentage_positions=True)
        
        # "unexpected end of stream" detektálása - speciális jelzés (None)
        if extracted_frames is None:
            print("  ✗ VirtualDub2: 'unexpected end of stream' - videó újrakódolásra szorul!")
            return None  # Speciális érték: újrakódolás szükséges
        
        if not extracted_frames:
            print("  ✗ Nem sikerült frame-eket exportálni!")
            return False
        
        if stop_event.is_set():
            raise EncodingStopped()
        
        print(f"\n3. Frame tartalom ellenőrzés ({len(extracted_frames)} db)...")
        valid_frames = 0
        black_frames = 0
        
        for frame_path in extracted_frames:
            if stop_event.is_set():
                raise EncodingStopped()
            print(f"    Vizsgálat: {frame_path.name}")
            is_black = is_frame_black_or_empty(frame_path)
            
            if not is_black:
                valid_frames += 1
            else:
                black_frames += 1
        
        success_rate = valid_frames / len(extracted_frames)
        min_success_rate = 0.33 if encoder == 'svt-av1' else 0.4
        
        is_valid = success_rate >= min_success_rate

        print(f"\n4. Frame statisztika (forrás/cél)...")
        if source_path:
            try:
                source_path_obj = Path(source_path)
            except (OSError, ValueError, TypeError):
                source_path_obj = None
            if source_path_obj and source_path_obj.exists():
                try:
                    source_frame_count = get_video_frame_count(source_path_obj)
                    if source_frame_count is not None:
                        print(f"    Forrás frame-ek: {source_frame_count}")
                    else:
                        print("    Forrás frame-ek: ismeretlen")
                except Exception as e:
                    print(f"    ✗ Forrás frame lekérdezés hiba: {e}")
            else:
                print("    Forrás frame-ek: forrás fájl nem elérhető")
        else:
            print("    Forrás frame-ek: nincs megadva")

        try:
            encoded_frame_count = get_video_frame_count(video_path)
            if encoded_frame_count is not None:
                print(f"    Cél frame-ek: {encoded_frame_count}")
            else:
                print("    Cél frame-ek: ismeretlen")
        except Exception as e:
            print(f"    ✗ Cél frame lekérdezés hiba: {e}")
            encoded_frame_count = None

        if source_frame_count and encoded_frame_count:
            allowed_diff = max(FRAME_MISMATCH_MIN_DIFF, int(source_frame_count * FRAME_MISMATCH_RATIO))
            diff = encoded_frame_count - source_frame_count
            if frames_significantly_different(source_frame_count, encoded_frame_count):
                frame_count_warning = True
                print(f"    ⚠ Jelentős eltérés a frame számokban (különbség: {diff:+d}, tolerancia: +/-{allowed_diff})")
            else:
                print(f"    ✓ Frame eltérés a tolerancián belül (különbség: {diff:+d})")

        print(f"\n5. Utolsó frame ellenőrzés...")
        last_frame_ok = False
        last_frame_warning = False
        last_frame_path = temp_dir / "frame_last.png"

        if last_frame_path.exists():
            try:
                last_frame_path.unlink()
            except OSError:
                pass

        vdub_success = export_last_frame_with_vdub2(
            video_path,
            last_frame_path,
            frame_count_hint=encoded_frame_count,
            stop_event=stop_event
        )

        if vdub_success:
            try:
                with Image.open(last_frame_path) as last_img:
                    last_img.verify()
                print(f"    ✓ Utolsó frame beolvasható VirtualDub2-vel ({last_frame_path.name})")
                last_frame_ok = True
            except Exception as exc:
                print(f"    ✗ VirtualDub2 által exportált utolsó frame sérült: {exc}")
                last_frame_ok = False
        else:
            print("    ⚠ VirtualDub2 nem tudta kinyerni az utolsó frame-et – FFmpeg fallback")

        def run_last_frame_attempt(cmd, description):
            print(description)
            print(f"      Parancs: {' '.join(cmd)}")
            try:
                if stop_event is not None and stop_event.is_set():
                    raise EncodingStopped()
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=240, startupinfo=get_startup_info())
                if result.stdout:
                    print("      FFmpeg STDOUT:")
                    print("        " + "\n        ".join(result.stdout.strip().splitlines()))
                if result.stderr:
                    print("      FFmpeg STDERR:")
                    print("        " + "\n        ".join(result.stderr.strip().splitlines()))
                if result.returncode != 0:
                    err_text = result.stderr.strip() or result.stdout.strip() or str(result.returncode)
                    print(f"    ✗ FFmpeg hiba: {err_text}")
                    return "error"
                if not last_frame_path.exists():
                    print("    ⚠ Utolsó frame fájl nem készült el – ellenőrzés kihagyva")
                    try:
                        parent_dir = Path(last_frame_path).parent
                        if parent_dir.exists():
                            contents = os.listdir(parent_dir)
                            print(f"      ⚠ Cél könyvtár tartalma: {contents}")
                    except OSError:
                        pass
                    return "missing"
                try:
                    with Image.open(last_frame_path) as last_img:
                        last_img.verify()
                    print(f"    ✓ Utolsó frame beolvasható ({last_frame_path.name})")
                    return "success"
                except Exception as exc:
                    print(f"    ✗ Utolsó frame beolvasási hiba: {exc}")
                    return "error"
            except EncodingStopped:
                raise
            except Exception as exc:
                print(f"    ✗ Utolsó frame export hibázott: {exc}")
                return "error"

        if not last_frame_ok:
            primary_cmd = [
                FFMPEG_PATH, '-hide_banner', '-loglevel', 'error', '-y',
                '-sseof', '-0.1',
                '-i', os.fspath(video_path),
                '-frames:v', '1',
                os.fspath(last_frame_path)
            ]
            attempt_result = run_last_frame_attempt(primary_cmd, "    FFmpeg utolsó frame export indítása...")
            if attempt_result == "success":
                last_frame_ok = True
            else:
                if attempt_result == "missing":
                    last_frame_warning = True
                fallback_cmd = [
                    FFMPEG_PATH, '-hide_banner', '-loglevel', 'error', '-y',
                    '-i', os.fspath(video_path),
                    '-vf'
                ]
                encoded_count = encoded_frame_count
                if encoded_count and encoded_count > 0:
                    last_index = max(encoded_count - 1, 0)
                    fallback_cmd.append(f"select=eq(n\\,{last_index})")
                else:
                    fallback_cmd.append("select=eq(n\\,n)")
                fallback_cmd.extend([
                    '-vsync', '0',
                    '-frames:v', '1',
                    os.fspath(last_frame_path)
                ])
                fallback_result = run_last_frame_attempt(fallback_cmd, "    FFmpeg utolsó frame export (végigfutás)...")
                if fallback_result == "success":
                    last_frame_ok = True
                    last_frame_warning = False
                elif fallback_result == "missing":
                    last_frame_warning = True
                else:
                    last_frame_ok = False

        if not last_frame_ok and not last_frame_warning:
            is_valid = False

        print(f"\n{'='*60}")
        print(f"VALIDÁCIÓ EREDMÉNYE:")
        file_size_mb = file_size / (1024**2)
        print(f"  Fájlméret: OK ({format_localized_number(file_size_mb, decimals=2)} MB)")
        print(f"  VirtualDub2 frame export: {len(extracted_frames)}/4 OK")
        success_percent = format_localized_number(success_rate * 100, decimals=1)
        print(f"  Valódi tartalom: {valid_frames}/{len(extracted_frames)} ({success_percent}%)")
        print(f"  Fekete/üres: {black_frames}")
        print(f"  Követelmény: ≥{min_success_rate*100:.0f}% valódi tartalom")
        if source_frame_count is not None or encoded_frame_count is not None:
            src_display = str(source_frame_count) if source_frame_count is not None else "ismeretlen"
            dst_display = str(encoded_frame_count) if encoded_frame_count is not None else "ismeretlen"
            print(f"  Frame-ek (forrás/cél): {src_display} / {dst_display}")
            if source_frame_count and encoded_frame_count:
                diff = encoded_frame_count - source_frame_count
                status_text = "⚠ eltérés" if frame_count_warning else "OK"
                print(f"  Frame különbség: {diff:+d} ({status_text})")
        if last_frame_warning:
            print(f"  Utolsó frame: ⚠ Nem sikerült ellenőrizni (fájl nem jött létre)")
        else:
            print(f"  Utolsó frame: {'OK' if last_frame_ok else '✗ HIBA'}")
        print(f"  Végső eredmény: {'✓ ÉRVÉNYES' if is_valid else '✗ ÉRVÉNYTELEN'}")
        print(f"{'='*60}")
        
        debug_pause(
            f"Validáció kész: {'ÉRVÉNYES' if is_valid else 'ÉRVÉNYTELEN'} ({valid_frames}/{len(extracted_frames)} jó)",
            "Temp könyvtár törlése" if not DEBUG_MODE else "Temp MEGŐRIZVE (debug)",
            f"Temp: {temp_dir}, Videó: {video_path}"
        )
        
        return is_valid
    except EncodingStopped:
        raise
    finally:
        if not DEBUG_MODE:
            try:
                shutil.rmtree(temp_dir)
            except (OSError, PermissionError, FileNotFoundError):
                pass
        else:
            print(f"  🛑 DEBUG: Temp MEGŐRIZVE: {temp_dir}")

def find_video_files(root_dir, include_av1=False):
    """Recursively find video files in a directory.
    
    Args:
        root_dir: Path to the root directory.
        include_av1: If False, skips files ending with .av1.
        
    Returns:
        list: List of found video files as Path objects.
    """
    video_files = []
    root_path = Path(root_dir)
    for file_path in root_path.rglob('*'):
        if file_path.is_file() and file_path.suffix.lower() in VIDEO_EXTENSIONS:
            # Kihagyjuk a .ab-av1-* almappákban lévő fájlokat (ab-av1 temp fájlok)
            path_parts = file_path.parts
            if any('.ab-av1-' in part for part in path_parts):
                continue
            
            if include_av1:
                # Ha include_av1=True, akkor minden videó fájlt hozzáadunk
                video_files.append(file_path)
            else:
                # Alapértelmezett: kihagyjuk az .av1 fájlokat
                if not file_path.stem.endswith('.av1'):
                    video_files.append(file_path)
    return video_files

def get_output_filename(input_path, source_root, dest_root):
    """Determine the output file path.
    
    Args:
        input_path: Path to the input video (Path).
        source_root: Source root directory (Path or None).
        dest_root: Destination root directory (Path or None).
        
    Returns:
        Path: Output file path (with .av1.mkv extension).
    """
    if dest_root is None:
        return input_path.parent / f"{input_path.stem}.av1.mkv"
    else:
        source_path = Path(source_root)
        dest_path = Path(dest_root)
        relative_path = input_path.relative_to(source_path)
        new_filename = f"{input_path.stem}.av1.mkv"
        output_path = dest_path / relative_path.parent / new_filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        return output_path


def get_copy_filename(input_path, source_root, dest_root):
    """Generate output path with ORIGINAL extension (for copy fallback).
    
    Used when encoding fails and video must be copied unchanged.
    Preserves original filename and extension (e.g. video.mp4 stays video.mp4).
    
    Args:
        input_path: Path to input video (Path).
        source_root: Source root directory (Path or None).
        dest_root: Destination root directory (Path or None).
        
    Returns:
        Path: Output path with original extension.
    """
    if dest_root is None:
        # Same directory: keep original path
        return input_path
    else:
        source_path = Path(source_root)
        dest_path = Path(dest_root)
        relative_path = input_path.relative_to(source_path)
        # Preserve original filename AND extension
        output_path = dest_path / relative_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        return output_path


def is_misnamed_copy(source_path, dest_av1_path):
    """Check if .av1.mkv file is actually unchanged copy of source.
    
    Detects cases where a video was copied (not encoded) but incorrectly
    saved with .av1.mkv extension. Uses file size comparison as the
    primary indicator.
    
    Args:
        source_path: Original source video path (Path).
        dest_av1_path: Destination .av1.mkv file path (Path).
        
    Returns:
        bool: True if dest file is unchanged copy (same size as source).
    """
    # Check both files exist
    if not source_path.exists() or not dest_av1_path.exists():
        return False
    
    # Check extension is .av1.mkv
    if dest_av1_path.suffix.lower() != '.mkv':
        return False
    if '.av1' not in dest_av1_path.stem:
        return False
    
    # Compare file sizes (most reliable indicator of unchanged copy)
    try:
        source_size = source_path.stat().st_size
        dest_size = dest_av1_path.stat().st_size
        
        # Same size = likely unchanged copy  
        # Encoded files are typically 50-75% of original size
        return source_size == dest_size
    except (OSError, PermissionError):
        return False


def rename_misnamed_copy_file(dest_av1_path, source_path, logger=None):
    """Rename misnamed .av1.mkv copy to original extension.
    
    Takes a .av1.mkv file that is actually an unchanged copy and renames
    it to match the original source file's extension.
    
    Args:
        dest_av1_path: Current .av1.mkv file path (Path).
        source_path: Original source file for extension reference (Path).
        logger: Optional logger for console output (ConsoleLogger).
        
    Returns:
        Path: New path with original extension, or None on error.
    """
    try:
        # Generate new name with original extension
        original_ext = source_path.suffix
        # Remove .av1 from stem
        new_stem = dest_av1_path.stem.replace('.av1', '')
        new_name = new_stem + original_ext
        new_path = dest_av1_path.with_name(new_name)
        
        # Check if target already exists
        if new_path.exists() and new_path != dest_av1_path:
            if logger:
                with console_redirect(logger):
                    print(f"⚠ Cél fájl már létezik: {new_path.name}")
            return None
        
        # Atomic rename
        dest_av1_path.rename(new_path)
        
        if logger:
            with console_redirect(logger):
                print(f"✓ Átnevezve: {dest_av1_path.name} → {new_path.name}")
        
        return new_path
        
    except (OSError, PermissionError) as e:
        if logger:
            with console_redirect(logger):
                print(f"✗ Átnevezés hiba: {e}")
        return None


def verify_and_copy_subtitles(source_path, dest_path, logger=None):
    """Verify and copy missing subtitle files from source to dest location.
    
    Ensures all subtitle files associated with the source video are also
    present at the destination location. Copies any missing subtitles.
    
    Args:
        source_path: Source video path (Path).
        dest_path: Destination video path (Path).
        logger: Optional logger for console output (ConsoleLogger).
        
    Returns:
        int: Number of subtitles copied.
    """
    # Find source subtitles
    source_subs = find_subtitle_files(source_path)
    if not source_subs:
        return 0
    
    copied_count = 0
    
    for sub_path, lang_code in source_subs:
        # Generate destination subtitle name
        if lang_code:
            # Insert language code before extension
            dest_sub_name = f"{dest_path.stem}.{lang_code}{sub_path.suffix}"
        else:
            dest_sub_name = dest_path.stem + sub_path.suffix
        
        dest_sub_path = dest_path.parent / dest_sub_name
        
        # Copy if doesn't exist
        if not dest_sub_path.exists():
            try:
                shutil.copy2(sub_path, dest_sub_path)
                copied_count += 1
                if logger:
                    with console_redirect(logger):
                        print(f"✓ Felirat másolva: {dest_sub_path.name}")
            except (OSError, PermissionError) as e:
                if logger:
                    with console_redirect(logger):
                        print(f"⚠ Felirat másolás hiba: {e}")
    
    return copied_count


def run_crf_search(input_path, encoder='av1_nvenc', initial_min_vmaf=None, vmaf_step=None, max_encoded_percent=None, progress_callback=None, logger=None, stop_event=None, svt_preset=2):
    """Run CRF search using ab-av1 to find optimal encoding settings.
    
    Args:
        input_path: Path to input video.
        encoder: Encoder to use ('av1_nvenc' or 'svt-av1').
        initial_min_vmaf: Target VMAF score.
        vmaf_step: Step size for VMAF adjustment.
        max_encoded_percent: Maximum allowed size percentage of source.
        progress_callback: Callback for progress updates.
        logger: Logger instance.
        stop_event: Event to stop search.
        svt_preset: SVT-AV1 preset value.
        
    Returns:
        int: Optimal CRF/CQ value or None if failed.
    """

    # Ellenőrizzük, hogy a fájl létezik-e és a helyes fájl-e
    if not input_path.exists():
        raise FileNotFoundError(f"CRF kereséshez megadott fájl nem létezik: {input_path}")
    
    # Ellenőrizzük, hogy az ab-av1.exe létezik-e
    if not Path(ABAV1_PATH).exists() and not shutil.which(ABAV1_PATH):
        error_msg = f"VÉGZETES HIBA: Az ab-av1.exe nem található! Útvonal: {ABAV1_PATH}\n\nA program nem tudja elindítani az ab-av1.exe-t, ezért a CRF keresés nem lehetséges.\n\nEllenőrizd, hogy az ab-av1.exe létezik-e a megadott útvonalon, vagy állítsd be a helyes útvonalat a beállításokban."
        print(f"\n{'='*80}")
        print(f"⚠⚠⚠ VÉGZETES HIBA ⚠⚠⚠")
        print(f"{'='*80}")
        print(error_msg)
        print(f"{'='*80}\n")
        if logger:
            try:
                logger.write(f"\n{'='*80}\n")
                logger.write(f"⚠⚠⚠ VÉGZETES HIBA ⚠⚠⚠\n")
                logger.write(f"{'='*80}\n")
                logger.write(f"{error_msg}\n")
                logger.write(f"{'='*80}\n\n")
                logger.flush()
            except Exception:
                pass
        if LOG_WRITER:
            try:
                LOG_WRITER.write(f"\n{'='*80}\n")
                LOG_WRITER.write(f"⚠⚠⚠ VÉGZETES HIBA ⚠⚠⚠\n")
                LOG_WRITER.write(f"{'='*80}\n")
                LOG_WRITER.write(f"{error_msg}\n")
                LOG_WRITER.write(f"{'='*80}\n\n")
                LOG_WRITER.flush()
            except Exception:
                pass
        raise FileNotFoundError(error_msg)
    
    # KRITIKUS VÉDELEM: Abszolút útvonal és fájl azonosítók mentése CRF keresés ELŐTT
    # Ez védi meg, hogy ne keveredjenek a CRF értékek különböző videók között
    input_absolute_start = input_path.absolute()
    input_str = os.fspath(input_absolute_start)
    
    # KRITIKUS: Fájl azonosítók mentése (méret, módosítási dátum) - védelem a fájl cseréje ellen
    try:
        input_stat_start = input_path.stat()
        input_size_start = input_stat_start.st_size
        input_mtime_start = input_stat_start.st_mtime
    except (OSError, PermissionError) as e:
        raise FileNotFoundError(f"CRF keresés: nem sikerült a fájl stat() hívása: {input_path} - {e}")
    
    # Részletes logolás: ellenőrizzük, hogy a PONTOS fájlt használjuk
    print(f"🔍 CRF KERESÉS INDÍTÁSA:")
    print(f"   Fájl név: {input_path.name}")
    print(f"   Abszolút útvonal: {input_str}")
    print(f"   Fájl méret: {input_size_start:,} bytes")
    print(f"   Módosítás: {datetime.fromtimestamp(input_mtime_start).strftime('%Y-%m-%d %H:%M:%S')}")
    
    initial_min_vmaf, vmaf_step, max_encoded_percent = resolve_encoding_defaults(initial_min_vmaf, vmaf_step, max_encoded_percent)

    min_vmaf = initial_min_vmaf
    min_vmaf_threshold = 85.0

    if stop_event is None:
        stop_event = STOP_EVENT
    
    if stop_event.is_set():
        raise EncodingStopped()
    
    if progress_callback:
        encoder_label = "NVENC" if encoder == 'av1_nvenc' else "SVT-AV1"
        min_vmaf_str = format_localized_number(min_vmaf, decimals=1)
        progress_callback(f"{encoder_label} CRF keresés (VMAF: {min_vmaf_str})")
    
    while min_vmaf >= min_vmaf_threshold:
        if stop_event.is_set():
            raise EncodingStopped()
        # FONTOS: Abszolút útvonalat használunk, hogy biztosan a helyes fájlt használjuk
        # A cwd=input_path.parent csak a working directory-t állítja be, de az -i paraméterben abszolút útvonal van
        if encoder == 'svt-av1':
            ab_av1_cmd = [ABAV1_PATH, 'crf-search', '-i', input_str, '-e', 'svt-av1', '--min-vmaf', str(min_vmaf), '--preset', str(svt_preset), '--max-encoded-percent', str(int(max_encoded_percent))]
        else:
            ab_av1_cmd = [ABAV1_PATH, 'crf-search', '-i', input_str, '-e', 'av1_nvenc', '--min-vmaf', str(min_vmaf), '--max-encoded-percent', str(int(max_encoded_percent))]
        
        if stop_event.is_set():
            raise EncodingStopped()
        
        try:
            print(f"\n{'='*80}")
            print(f"🎬 AB-AV1 CRF SEARCH ({encoder}) - Min VMAF: {min_vmaf}")
            print(f"{'='*80}")
            print(f"FÁJL: {input_path.name}")
            print(f"ABSZOLÚT ÚTVONAL: {input_str}")
            print(f"PARANCS: {' '.join(ab_av1_cmd)}")
            print(f"{'='*80}\n")

            # FONTOS: NEM állítjuk be a cwd-t, mert lehetnek egyező fájlnevek különböző mappákban
            # Az abszolút útvonal használata biztosítja, hogy a helyes fájlt használjuk
            process = subprocess.Popen(
                ab_av1_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1,
                startupinfo=get_startup_info()
            )
            
            # Process regisztráció
            with ACTIVE_PROCESSES_LOCK:
                ACTIVE_PROCESSES.append(process)
            
            full_output = []
            try:
                for line in process.stdout:
                    print(line.rstrip())
                    full_output.append(line)
            except (OSError, IOError, BrokenPipeError, UnicodeDecodeError) as e:
                print(f"Process output olvasás hiba: {e}")
            
            try:
                process.wait(timeout=600)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            finally:
                # Process törlése a listából
                with ACTIVE_PROCESSES_LOCK:
                    if process in ACTIVE_PROCESSES:
                        ACTIVE_PROCESSES.remove(process)
            
            full_output_text = ''.join(full_output)
            
            print(f"\n{'='*80}")
            print(f"AB-AV1 BEFEJEZVE - Return code: {process.returncode}")
            print(f"{'='*80}\n")
            
            if stop_event.is_set():
                raise EncodingStopped()
            
            crf_pattern = r'crf\s+(\d+)\s+VMAF\s+([\d.]+)\s+predicted\s+video\s+stream\s+size\s+[\d.]+\s+\w+\s+\((\d+)%\)'
            all_crf_results = re.findall(crf_pattern, full_output_text)
            
            has_failed = 'Failed to find a suitable crf' in full_output_text or 'Error: Failed to find' in full_output_text
            
            if has_failed:
                print(f"⚠ Ab-av1 'Failed to find' hiba, keresünk használható CRF-et...")
                
                if all_crf_results:
                    valid_crfs = [
                        (int(crf), float(vmaf), int(size_pct)) 
                        for crf, vmaf, size_pct in all_crf_results 
                        if float(vmaf) >= min_vmaf and int(size_pct) <= max_encoded_percent
                    ]
                    
                    if valid_crfs:
                        best_crf = max(valid_crfs, key=lambda x: x[0])
                        best_crf_str = format_localized_number(best_crf[0], decimals=1)
                        best_vmaf_str = format_localized_number(best_crf[1], decimals=1)
                        best_size_str = format_localized_number(best_crf[2], decimals=1)
                        min_vmaf_str = format_localized_number(min_vmaf, decimals=1)
                        print(f"✓ CRF TALÁLVA (Failed helyett): CRF {best_crf_str}, VMAF {best_vmaf_str}, {best_size_str}%")
                        
                        debug_pause(
                            f"Ab-av1 CRF: {best_crf_str} (VMAF: {best_vmaf_str})",
                            f"FFmpeg kódolás CRF {best_crf_str}-val",
                            f"Encoder: {encoder}, Min VMAF: {min_vmaf_str}"
                        )
                        
                        # KRITIKUS VÉDELEM: Ellenőrzés a CRF visszaadása ELŐTT
                        # Biztosítjuk, hogy UGYANAZ a fájl van, mint a CRF keresés kezdetekor
                        if not input_path.exists():
                            raise FileNotFoundError(f"VÉGZETES HIBA: A forrás fájl ELTŰNT a CRF keresés során!\n"
                                                   f"Fájl: {input_str}\n"
                                                   f"Ez azt jelenti, hogy a CRF érték ({best_crf[0]}) érvénytelen!")
                        
                        # Ellenőrzés: UGYANAZ az abszolút útvonal?
                        input_absolute_end = input_path.absolute()
                        if input_absolute_end != input_absolute_start:
                            raise ValueError(f"VÉGZETES HIBA: A forrás fájl MEGVÁLTOZOTT a CRF keresés során!\n"
                                           f"CRF keresés kezdetekor: {input_absolute_start}\n"
                                           f"CRF keresés végén: {input_absolute_end}\n"
                                           f"Ez azt jelenti, hogy a CRF érték ({best_crf[0]}) MÁS VIDEÓHOZ tartozik!\n"
                                           f"A program azonnal leáll a biztonság érdekében.")
                        
                        # Ellenőrzés: UGYANAZ a fájl méret és módosítási dátum?
                        try:
                            input_stat_end = input_path.stat()
                            input_size_end = input_stat_end.st_size
                            input_mtime_end = input_stat_end.st_mtime
                            
                            if input_size_end != input_size_start:
                                raise ValueError(f"VÉGZETES HIBA: A forrás fájl MÉRETE MEGVÁLTOZOTT a CRF keresés során!\n"
                                               f"Méret kezdetkor: {input_size_start:,} bytes\n"
                                               f"Méret végén: {input_size_end:,} bytes\n"
                                               f"Ez azt jelenti, hogy a fájl módosult, és a CRF érték ({best_crf[0]}) érvénytelen!")
                            
                            if abs(input_mtime_end - input_mtime_start) > 1.0:
                                raise ValueError(f"VÉGZETES HIBA: A forrás fájl MÓDOSÍTÁSI DÁTUMA MEGVÁLTOZOTT a CRF keresés során!\n"
                                               f"Dátum kezdetkor: {datetime.fromtimestamp(input_mtime_start).strftime('%Y-%m-%d %H:%M:%S')}\n"
                                               f"Dátum végén: {datetime.fromtimestamp(input_mtime_end).strftime('%Y-%m-%d %H:%M:%S')}\n"
                                               f"Ez azt jelenti, hogy a fájl módosult, és a CRF érték ({best_crf[0]}) érvénytelen!")
                        except (OSError, PermissionError) as e:
                            raise FileNotFoundError(f"VÉGZETES HIBA: Nem sikerült ellenőrizni a fájlt a CRF keresés végén: {e}")
                        
                        return (float(best_crf[0]), float(best_crf[1]))
                    else:
                        print(f"⚠ Nincs VMAF >= {min_vmaf_str} ÉS fájl <= {format_localized_number(max_encoded_percent, decimals=1)}%")
                
                next_vmaf_str = format_localized_number(min_vmaf - vmaf_step, decimals=1)
                print(f"⚠ VMAF csökkentés: {min_vmaf_str} → {next_vmaf_str}")
                min_vmaf -= vmaf_step
                if progress_callback:
                    encoder_label = "NVENC" if encoder == 'av1_nvenc' else "SVT-AV1"
                    progress_callback(f"{encoder_label} CRF keresés (VMAF fallback: {format_localized_number(min_vmaf, decimals=1)})")
                continue
            
            if process.returncode == 0:
                all_crf_vmaf_matches = re.findall(r'crf\s+(\d+(?:\.\d+)?)\s+VMAF\s+([\d.]+)', full_output_text)
                
                if all_crf_vmaf_matches:
                    last_match = all_crf_vmaf_matches[-1]
                    crf_value = float(last_match[0])
                    actual_vmaf = float(last_match[1])
                    print(f"✓ CRF TALÁLVA: {crf_value} (VMAF: {actual_vmaf})")
                    
                    debug_pause(
                        f"Ab-av1 CRF: {crf_value} (VMAF: {actual_vmaf})",
                        f"FFmpeg kódolás CRF {crf_value}-val",
                        f"Encoder: {encoder}"
                    )
                    
                    # KRITIKUS VÉDELEM: Ellenőrzés a CRF visszaadása ELŐTT
                    if not input_path.exists():
                        raise FileNotFoundError(f"VÉGZETES HIBA: A forrás fájl ELTŰNT a CRF keresés során!\n"
                                               f"Fájl: {input_str}\n"
                                               f"Ez azt jelenti, hogy a CRF érték ({crf_value}) érvénytelen!")
                    
                    input_absolute_end = input_path.absolute()
                    if input_absolute_end != input_absolute_start:
                        raise ValueError(f"VÉGZETES HIBA: A forrás fájl MEGVÁLTOZOTT a CRF keresés során!\n"
                                       f"CRF keresés kezdetekor: {input_absolute_start}\n"
                                       f"CRF keresés végén: {input_absolute_end}\n"
                                       f"Ez azt jelenti, hogy a CRF érték ({crf_value}) MÁS VIDEÓHOZ tartozik!")
                    
                    try:
                        input_stat_end = input_path.stat()
                        if input_stat_end.st_size != input_size_start or abs(input_stat_end.st_mtime - input_mtime_start) > 1.0:
                            raise ValueError(f"VÉGZETES HIBA: A forrás fájl MÓDOSULT a CRF keresés során!\n"
                                           f"Méret: {input_size_start:,} → {input_stat_end.st_size:,} bytes\n"
                                           f"Dátum: {datetime.fromtimestamp(input_mtime_start).strftime('%Y-%m-%d %H:%M:%S')} → {datetime.fromtimestamp(input_stat_end.st_mtime).strftime('%Y-%m-%d %H:%M:%S')}")
                    except (OSError, PermissionError) as e:
                        raise FileNotFoundError(f"VÉGZETES HIBA: Nem sikerült ellenőrizni a fájlt: {e}")
                    
                    return (crf_value, actual_vmaf)
                else:
                    all_crf_successful = re.findall(r'crf\s+(\d+(?:\.\d+)?)\s+successful', full_output_text)
                    if all_crf_successful:
                        crf_value = float(all_crf_successful[-1])
                        crf_value_str = format_localized_number(crf_value, decimals=1)
                        print(f"✓ CRF TALÁLVA (utolsó successful): {crf_value_str}")
                        
                        debug_pause(
                            f"Ab-av1 CRF: {crf_value_str}",
                            f"FFmpeg kódolás",
                            f"Encoder: {encoder}"
                        )
                        
                        return (crf_value, min_vmaf)
                    else:
                        current_vmaf_str = format_localized_number(min_vmaf, decimals=1)
                        next_vmaf_str = format_localized_number(min_vmaf - vmaf_step, decimals=1)
                        print(f"⚠ Nincs CRF, VMAF csökkentés: {current_vmaf_str} → {next_vmaf_str}")
                        min_vmaf -= vmaf_step
                        if progress_callback:
                            encoder_label = "NVENC" if encoder == 'av1_nvenc' else "SVT-AV1"
                            progress_callback(f"{encoder_label} CRF keresés (VMAF fallback: {format_localized_number(min_vmaf, decimals=1)})")
                        continue
            else:
                current_vmaf_str = format_localized_number(min_vmaf, decimals=1)
                next_vmaf_str = format_localized_number(min_vmaf - vmaf_step, decimals=1)
                print(f"⚠ Ab-av1 hiba (rc: {process.returncode}), VMAF csökkentés: {current_vmaf_str} → {next_vmaf_str}")
                min_vmaf -= vmaf_step
                if progress_callback:
                    encoder_label = "NVENC" if encoder == 'av1_nvenc' else "SVT-AV1"
                    progress_callback(f"{encoder_label} CRF keresés (VMAF fallback: {format_localized_number(min_vmaf, decimals=1)})")
                continue
                
        except subprocess.TimeoutExpired:
            print(f"✗ Ab-av1 timeout!")
            cleanup_ab_av1_temp_dirs(input_path.parent)
            break
        except FileNotFoundError as e:
            # WinError 2 vagy FileNotFoundError - ab-av1.exe nem található
            error_msg = f"VÉGZETES HIBA: Az ab-av1.exe nem található vagy nem indítható!\n\nHiba: {e}\n\nÚtvonal: {ABAV1_PATH}\n\nA program nem tudja elindítani az ab-av1.exe-t, ezért a CRF keresés nem lehetséges.\n\nEllenőrizd, hogy az ab-av1.exe létezik-e a megadott útvonalon, vagy állítsd be a helyes útvonalat a beállításokban."
            print(f"\n{'='*80}")
            print(f"⚠⚠⚠ VÉGZETES HIBA ⚠⚠⚠")
            print(f"{'='*80}")
            print(error_msg)
            print(f"{'='*80}\n")
            if logger:
                try:
                    logger.write(f"\n{'='*80}\n")
                    logger.write(f"⚠⚠⚠ VÉGZETES HIBA ⚠⚠⚠\n")
                    logger.write(f"{'='*80}\n")
                    logger.write(f"{error_msg}\n")
                    logger.write(f"{'='*80}\n\n")
                    logger.flush()
                except Exception:
                    pass
            if LOG_WRITER:
                try:
                    LOG_WRITER.write(f"\n{'='*80}\n")
                    LOG_WRITER.write(f"⚠⚠⚠ VÉGZETES HIBA ⚠⚠⚠\n")
                    LOG_WRITER.write(f"{'='*80}\n")
                    LOG_WRITER.write(f"{error_msg}\n")
                    LOG_WRITER.write(f"{'='*80}\n\n")
                    LOG_WRITER.flush()
                except Exception:
                    pass
            cleanup_ab_av1_temp_dirs(input_path.parent)
            raise FileNotFoundError(error_msg) from e
        except (subprocess.SubprocessError, OSError, ValueError, TypeError, AttributeError) as e:
            # Egyéb hibák (nem FileNotFoundError) - lehet, hogy VMAF fallback probléma
            error_str = str(e)
            # Ha WinError 2 van az exception szövegében, akkor is ab-av1.exe probléma
            if "WinError 2" in error_str or "[WinError 2]" in error_str or "The system cannot find the file specified" in error_str:
                error_msg = f"VÉGZETES HIBA: Az ab-av1.exe nem található vagy nem indítható!\n\nHiba: {e}\n\nÚtvonal: {ABAV1_PATH}\n\nA program nem tudja elindítani az ab-av1.exe-t, ezért a CRF keresés nem lehetséges.\n\nEllenőrizd, hogy az ab-av1.exe létezik-e a megadott útvonalon, vagy állítsd be a helyes útvonalat a beállításokban."
                print(f"\n{'='*80}")
                print(f"⚠⚠⚠ VÉGZETES HIBA ⚠⚠⚠")
                print(f"{'='*80}")
                print(error_msg)
                print(f"{'='*80}\n")
                if logger:
                    try:
                        logger.write(f"\n{'='*80}\n")
                        logger.write(f"⚠⚠⚠ VÉGZETES HIBA ⚠⚠⚠\n")
                        logger.write(f"{'='*80}\n")
                        logger.write(f"{error_msg}\n")
                        logger.write(f"{'='*80}\n\n")
                        logger.flush()
                    except Exception:
                        pass
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"\n{'='*80}\n")
                        LOG_WRITER.write(f"⚠⚠⚠ VÉGZETES HIBA ⚠⚠⚠\n")
                        LOG_WRITER.write(f"{'='*80}\n")
                        LOG_WRITER.write(f"{error_msg}\n")
                        LOG_WRITER.write(f"{'='*80}\n\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
                cleanup_ab_av1_temp_dirs(input_path.parent)
                raise FileNotFoundError(error_msg) from e
            else:
                # Egyéb hiba - lehet, hogy VMAF fallback probléma
                print(f"✗ Ab-av1 exception: {e}")
                cleanup_ab_av1_temp_dirs(input_path.parent)
                break
    
    if stop_event.is_set():
        cleanup_ab_av1_temp_dirs(input_path.parent)
        raise EncodingStopped()

    cleanup_ab_av1_temp_dirs(input_path.parent)
    default_crf = 28 if encoder == 'av1_nvenc' else 32

    # Ha NVENC-nél elfogyott a VMAF fallback, akkor SVT-AV1 queue-ba helyezzük
    if encoder == 'av1_nvenc':
        print(f"⚠ NVENC VMAF fallback elfogyott → automatikusan SVT-AV1 queue-ba helyezés")
        return (default_crf, min_vmaf, True)  # True = fallback elfogyott

    # Ha SVT-AV1-nél is elfogyott a VMAF fallback, akkor nincs megfelelő CRF → egyszerű másolás
    print(f"⚠ SVT-AV1 VMAF fallback elfogyott → Nem talált megfelelő CRF értéket (VMAF >= 85.0 ÉS fájl <= 75%)")
    print(f"   Videó másolása átkódolás nélkül...")
    raise NoSuitableCRFFound("Nem talált megfelelő CRF értéket a megadott paraméterekhez")

def encode_single_attempt(input_path, output_path, cq_value, subtitle_files, encoder='av1_nvenc', status_callback=None, stop_event=None, vmaf_value=None, resize_enabled=False, resize_height=1080, audio_compression_enabled=False, audio_compression_method='fast', svt_preset=2, logger=None):
    """Execute a single encoding attempt with specified settings.
    
    Args:
        input_path: Path to input video.
        output_path: Path to output video.
        cq_value: CRF/CQ value to use.
        subtitle_files: List of subtitle files to include.
        encoder: Encoder name.
        status_callback: Callback for status updates.
        stop_event: Event to stop encoding.
        vmaf_value: Target VMAF (for metadata).
        resize_enabled: Whether to resize video.
        resize_height: Target height if resizing.
        audio_compression_enabled: Whether to compress audio.
        audio_compression_method: Audio compression method.
        svt_preset: SVT-AV1 preset.
        logger: Logger instance.
        
    Returns:
        bool: True if encoding successful, False otherwise.
    """
    # KÖZEPES JAVÍTÁS #8: Path sanitizálás biztonsági okokból - használjuk a sanitizált útvonalat
    try:
        input_str = sanitize_path(input_path)
    except (FileNotFoundError, ValueError) as e:
        raise ValueError(f"Invalid input path: {e}") from e
    
    # Output path nem kell, hogy létezzen, de validálni kell
    try:
        output_resolved = output_path.resolve()
        # Ellenőrizzük, hogy a parent directory létezik
        if not output_resolved.parent.exists():
            raise FileNotFoundError(f"Output directory does not exist: {output_resolved.parent}")
        output_str = os.fspath(output_resolved)
    except (OSError, RuntimeError) as e:
        raise ValueError(f"Invalid output path: {e}") from e
    
    # KÖZEPES JAVÍTÁS #8: Használjuk a sanitizált input_str-t a Path objektum helyett
    # Get video duration and fps for progress display
    duration_seconds, video_fps = get_video_info(Path(input_str))
    # KÖZEPES JAVÍTÁS #11: Biztonságos None érték kezelés
    if duration_seconds is None or duration_seconds <= 0:
        duration_seconds = 0
    if video_fps is None or video_fps <= 0:
        video_fps = 25.0  # Fallback fps
    duration_hours = int(duration_seconds // 3600)
    duration_mins = int((duration_seconds % 3600) // 60)
    duration_secs = int(duration_seconds % 60)
    
    # Progress tracking változók - biztonságos számítás
    total_frames = int(duration_seconds * video_fps) if duration_seconds > 0 and video_fps > 0 else 0
    
    ffmpeg_cmd = [FFMPEG_PATH, '-i', input_str]
    
    # ================================================================================
    # SUBTITLE VALIDÁLÁS - FFmpeg hiba megelőzése
    # ================================================================================
    # Validáljuk a feliratokat ENCODING ELŐTT, hogy elkerüljük az FFmpeg hibát
    # Ha egy felirat korrupt/érvénytelen, az FFmpeg elszáll beágyazáskor
    # 
    # Stratégia:
    # - Érvényes feliratok: FFmpeg-be beágyazás (encoding során)
    # - Érvénytelen feliratok: kihagyás beágyazásból, de másolás output mellé
    #   (a hívó function a _copy_invalid_subtitles()-ot használja erre)
    
    validated_subtitles = []
    skipped_subtitles = []
    
    # Logolás worker console-ra (részletes)
    if subtitle_files:
        if logger:
            with console_redirect(logger):
                print(f"\n{'='*80}")
                print(f"📋 FELIRAT VALIDÁLÁS")
                print(f"{'='*80}")
                print(f"Talállott feliratok száma: {len(subtitle_files)}")
        else:
            print(f"\n📋 Felirat validálás: {len(subtitle_files)} fájl")
    
    for sub_path, lang in subtitle_files:
        # Validálás: fájl méret, formátum, tartalom
        is_valid, reason = is_valid_subtitle_file(sub_path)
        
        if is_valid:
            validated_subtitles.append((sub_path, lang))
            # Sikeres validálás logolása
            if logger:
                with console_redirect(logger):
                    lang_display = f" ({lang})" if lang else ""
                    print(f"  ✓ {sub_path.name}{lang_display} - Érvényes")
        else:
            skipped_subtitles.append((sub_path, lang, reason))
            # RÉSZLETES hibajelentés worker console-ra
            if logger:
                with console_redirect(logger):
                    lang_display = f" ({lang})" if lang else ""
                    print(f"  ✗ {sub_path.name}{lang_display} - ÉRVÉNYTELEN")
                    print(f"     Hiba: {reason}")
                    print(f"     → Kihagyva az FFmpeg beágyazásból")
                    print(f"     → Átmásolásra kerül az output mellé")
            else:
                print(f"  ✗ {sub_path.name} - {reason}")
    
    # Csak a validált feliratokat ágyazzuk be az FFmpeg parancsba
    subtitle_files = validated_subtitles
    
    # ÖSSZEFOGLALÓ worker console-ra (FFmpeg encoding előtt)
    if logger:
        with console_redirect(logger):
            if skipped_subtitles:
                print(f"\n{'─'*80}")
                print(f"⚠ FELIRAT VALIDÁLÁS ÖSSZEGZÉSE:")
                print(f"   • Érvényes feliratok (FFmpeg beágyazás): {len(validated_subtitles)}")
                print(f"   • Érvénytelen feliratok (kihagyva): {len(skipped_subtitles)}")
                print(f"\n   Érvénytelen feliratok részletei:")
                for sub_path, lang, reason in skipped_subtitles:
                    lang_display = f" [{lang}]" if lang else ""
                    print(f"     - {sub_path.name}{lang_display}: {reason}")
                print(f"\n   ℹ Az érvénytelen feliratok átmásolásra kerülnek (nem beágyazva).")
                print(f"{'─'*80}\n")
            else:
                print(f"\n✓ Minden felirat érvényes ({len(validated_subtitles)} db)")
                print(f"{'='*80}\n")
    elif skipped_subtitles:
        print(f"\n📋 {len(validated_subtitles)} érvényes, {len(skipped_subtitles)} érvénytelen felirat")
    
    # ================================================================================
    
    for subtitle_path, _ in subtitle_files:
        # Subtitle path sanitizálás
        try:
            subtitle_str = sanitize_path(subtitle_path)
        except (FileNotFoundError, ValueError) as e:
            raise ValueError(f"Invalid subtitle path: {subtitle_path} - {e}") from e
        ffmpeg_cmd.extend(['-i', subtitle_str])
    
    # Hangdinamika kompresszió ellenőrzése
    use_audio_compression = False
    audio_51_stream_index = None
    compressed_audio_lang = None  # Az eredeti 5.1 hangsáv nyelve (a kompressziós hangsávhoz)
    if audio_compression_enabled:
        # KÖZEPES JAVÍTÁS #8: Használjuk a sanitizált input_str-t
        if needs_audio_compression(Path(input_str)):
            # Megkeressük a 5.1 hangsáv indexét az alapértelmezett nyelvhez
            default_lang, _, _ = get_audio_streams_info(Path(input_str))
            if default_lang:
                # KÖZEPES JAVÍTÁS #8: Használjuk a sanitizált input_str-t
                audio_51_stream_index = get_51_audio_stream_index(Path(input_str), default_lang)
                # Ha megtaláltuk a 5.1 hangsávot, használjuk a kompressziót
                if audio_51_stream_index is not None:
                    use_audio_compression = True
                    # Az eredeti 5.1 hangsáv nyelvének lekérdezése
                    try:
                        cmd = [
                            FFPROBE_PATH, '-v', 'error',
                            '-select_streams', f'a:{audio_51_stream_index}',
                            '-show_entries', 'stream_tags=language',
                            '-of', 'default=noprint_wrappers=1:nokey=1',
                            input_str  # Már sanitizálva van
                        ]
                        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=10, startupinfo=get_startup_info())
                        lang_raw = result.stdout.strip()
                        if lang_raw:
                            # Normalizáljuk a nyelv kódot (3 betűs -> 2 betűs, ha szükséges)
                            compressed_audio_lang = normalize_audio_lang(lang_raw)
                            # Ha 2 betűs, akkor a LANGUAGE_MAP-ból kérjük a 3 betűs verziót
                            if len(compressed_audio_lang) == 2 and compressed_audio_lang in LANGUAGE_MAP:
                                compressed_audio_lang = LANGUAGE_MAP[compressed_audio_lang]
                            else:
                                # Ha már 3 betűs, használjuk azt
                                compressed_audio_lang = lang_raw if len(lang_raw) == 3 else compressed_audio_lang
                    except (ValueError, TypeError, AttributeError, KeyError):
                        # Ha nem sikerül, az alapértelmezett nyelvet használjuk
                        if default_lang in LANGUAGE_MAP:
                            compressed_audio_lang = LANGUAGE_MAP[default_lang]
                        else:
                            compressed_audio_lang = default_lang
    
    # Video és hangsávok mapping
    ffmpeg_cmd.extend(['-map', '0:v:0'])
    
    # Eredeti hangsávok számának meghatározása (a kompressziós hangsáv indexéhez)
    original_audio_count = 0
    try:
        # KÖZEPES JAVÍTÁS #8: Használjuk a sanitizált input_str-t
        default_lang, _, _ = get_audio_streams_info(Path(input_str))
        if default_lang is not None:
            # FFprobe parancs az eredeti hangsávok számának lekérdezéséhez
            count_cmd = [
                FFPROBE_PATH, '-v', 'error',
                '-select_streams', 'a',
                '-show_entries', 'stream=index',
                '-of', 'default=noprint_wrappers=1:nokey=1',
                input_str  # Már sanitizálva van
            ]
            count_result = subprocess.run(count_cmd, capture_output=True, text=True, check=True, timeout=10, startupinfo=get_startup_info())
            original_audio_count = len([line for line in count_result.stdout.strip().split('\n') if line.strip()])
    except (subprocess.SubprocessError, ValueError, AttributeError):
        # Ha nem sikerül, feltételezzük, hogy 1 hangsáv van
        original_audio_count = 1
    
    # Mindig másoljuk az összes hangsávot
    ffmpeg_cmd.extend(['-map', '0:a?'])  # Összes hangsáv
    
    # Hangdinamika kompresszió filter hozzáadása (ha be van kapcsolva, hozzáadjuk a 5.1 hangsávot kompresszióval)
    audio_filter_complex = None
    compressed_audio_index = None  # Az utolsó hangsáv indexe (a kompressziós hangsáv)
    if use_audio_compression and audio_51_stream_index is not None:
        # Ha a combobox értéke fordított szöveg, konvertáljuk
        method = audio_compression_method
        if method == t('audio_compression_fast'):
            method = 'fast'
        elif method == t('audio_compression_dialogue'):
            method = 'dialogue'
        
        audio_filter = build_audio_conversion_filter(method)
        
        # Filter complex használata: a 5.1 hangsávot kompresszióval hozzáadjuk
        audio_filter_complex = f'[0:a:{audio_51_stream_index}]{audio_filter}[acompressed]'
        # A kompressziós hangsávot hozzáadjuk a mapping-hez
        ffmpeg_cmd.extend(['-map', '[acompressed]'])
        # Az utolsó hangsáv indexe (a kompressziós hangsáv) = eredeti hangsávok száma
        compressed_audio_index = original_audio_count
    
    # Beágyazott feliratok számának lekérdezése
    embedded_subtitle_count = 0
    try:
        count_cmd = [
            FFPROBE_PATH, '-v', 'error',
            '-select_streams', 's',
            '-show_entries', 'stream=index',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            input_str  # Már sanitizálva van
        ]
        count_result = subprocess.run(count_cmd, capture_output=True, text=True, check=True, timeout=10, startupinfo=get_startup_info())
        embedded_subtitle_count = len([line for line in count_result.stdout.strip().split('\n') if line.strip()])
    except (subprocess.SubprocessError, ValueError, AttributeError):
        # Ha nem sikerül, feltételezzük, hogy 0 beágyazott felirat van
        embedded_subtitle_count = 0
    
    ffmpeg_cmd.extend(['-map', '0:s?'])
    
    for idx in range(len(subtitle_files)):
        ffmpeg_cmd.extend(['-map', f'{idx+1}:0'])
    
    # Video filter hozzáadása, ha be van kapcsolva
    if resize_enabled:
        # A rövidebb oldal pixelszáma alapján méretezünk
        # KÖZEPES JAVÍTÁS #8: Használjuk a sanitizált input_str-t
        video_width, video_height = get_video_resolution(Path(input_str))
        if video_width and video_height and video_width > 0 and video_height > 0:
            # Rövidebb oldal meghatározása
            shorter_side = min(video_width, video_height)
            # A resize_height értéke most a rövidebb oldal pixelszáma
            target_shorter_side = resize_height
            
            # Arány számítása (ZeroDivisionError elkerülése)
            if shorter_side > 0:
                scale_ratio = target_shorter_side / shorter_side
            else:
                scale_ratio = 1.0
            
            # Új méretek számítása
            new_width = int(video_width * scale_ratio)
            new_height = int(video_height * scale_ratio)
            
            # Páros számokra kerekítés (videó kódoláshoz szükséges)
            new_width = new_width if new_width % 2 == 0 else new_width + 1
            new_height = new_height if new_height % 2 == 0 else new_height + 1
            
            ffmpeg_cmd.extend(['-vf', f'scale={new_width}:{new_height}'])
        else:
            # Ha nem sikerül a felbontás lekérdezése, régi módszer (magasság alapján)
            ffmpeg_cmd.extend(['-vf', f'scale=-2:{resize_height}'])
    
    # Audio filter complex hozzáadása, ha be van kapcsolva
    if audio_filter_complex:
        ffmpeg_cmd.extend(['-filter_complex', audio_filter_complex])
    
    # Video encoder beállítások
    if encoder == 'svt-av1':
        ffmpeg_cmd.extend(['-c:v', 'libsvtav1', '-preset', str(svt_preset), '-crf', str(int(cq_value)), '-g', '240', '-pix_fmt', 'yuv420p10le', '-stats_period', '0.5'])
        # Metadata hozzáadása SVT-AV1 esetén
        if vmaf_value is not None:
            vmaf_str = format_number_en(vmaf_value, decimals=1) if isinstance(vmaf_value, (int, float)) else str(vmaf_value)
            metadata_str = f"FFMPEG SVT-AV1 - CRF:{int(cq_value)} - Preset {svt_preset} - Planned VMAF: {vmaf_str}"
            ffmpeg_cmd.extend(['-metadata', f'Settings={metadata_str}'])
    else:
        ffmpeg_cmd.extend(['-c:v', 'av1_nvenc', '-preset', 'p7', '-tune', 'hq', '-rc', 'vbr', '-cq', str(int(cq_value)), '-multipass', 'fullres', '-pix_fmt', 'p010le', '-stats_period', '0.5'])
        # Metadata hozzáadása NVENC esetén
        if vmaf_value is not None:
            vmaf_str = format_number_en(vmaf_value, decimals=1) if isinstance(vmaf_value, (int, float)) else str(vmaf_value)
            metadata_str = f"FFMPEG NVENC - CQ:{int(cq_value)} - Preset 7 - Planned VMAF: {vmaf_str}"
            ffmpeg_cmd.extend(['-metadata', f'Settings={metadata_str}'])
    
    # Audio codec beállítás
    if use_audio_compression and audio_51_stream_index is not None and compressed_audio_index is not None:
        # Először minden hangsávra copy
        ffmpeg_cmd.extend(['-c:a', 'copy'])
        # Az utolsó hangsávra (a kompressziósra) AAC - stream specifier használata
        ffmpeg_cmd.extend([f'-c:a:{compressed_audio_index}', 'aac', f'-b:a:{compressed_audio_index}', '192k', f'-ac:{compressed_audio_index}', '2'])
        # Metadata hozzáadása a kompressziós hangsávhoz: nyelv és 2.0 jelölés
        if compressed_audio_lang:
            ffmpeg_cmd.extend([f'-metadata:s:a:{compressed_audio_index}', f'language={compressed_audio_lang}'])
        title_text = get_audio_conversion_title(method)
        ffmpeg_cmd.extend([f'-metadata:s:a:{compressed_audio_index}', f'title={title_text}'])
    else:
        ffmpeg_cmd.extend(['-c:a', 'copy'])
    
    # Beágyazott feliratok másolása - először az általános beállítás
    ffmpeg_cmd.extend(['-c:s', 'copy'])
    # Külső feliratok SRT-re konvertálása - pozitív stream index használata
    # A külső feliratok stream indexe: beágyazott_feliratok_száma + külső_felirat_index
    for idx in range(len(subtitle_files)):
        external_subtitle_stream_idx = embedded_subtitle_count + idx
        ffmpeg_cmd.extend([f'-c:s:{external_subtitle_stream_idx}', 'srt'])
    
    for idx, (subtitle_path, lang_part) in enumerate(subtitle_files):
        iso_lang = normalize_language_code(lang_part)
        # Külső feliratok stream indexe a beágyazottak után következik
        external_subtitle_stream_idx = embedded_subtitle_count + idx
        ffmpeg_cmd.extend([f'-metadata:s:s:{external_subtitle_stream_idx}', f'language={iso_lang}'])
        if lang_part:
            title = lang_part if '-' in lang_part else lang_part.upper()
            ffmpeg_cmd.extend([f'-metadata:s:s:{external_subtitle_stream_idx}', f'title={title}'])
    
    ffmpeg_cmd.extend(['-y', output_str])
    
    # FFmpeg parancs kiírása a konzolba
    # Ha van logger, akkor a console_redirect() context manager-en keresztül megy
    # Ha nincs logger, akkor a sys.stdout-ra megy (ritka eset)
    ffmpeg_cmd_str = ' '.join(ffmpeg_cmd)
    print(f"\n{'='*80}")
    print(f"🎬 FFMPEG PARANCS (CQ/CRF: {int(cq_value)}):")
    print(f"{'='*80}")
    print(ffmpeg_cmd_str)
    print(f"{'='*80}\n")
    
    if stop_event is None:
        stop_event = STOP_EVENT

    if stop_event.is_set():
        raise EncodingStopped()

    try:
        # FONTOS: NEM állítjuk be a cwd-t, mert lehetnek egyező fájlnevek különböző mappákban
        # Az abszolút útvonalak használata biztosítja, hogy a helyes fájlokat használjuk
        process = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True, bufsize=1, shell=False, startupinfo=get_startup_info())
        
        # Process regisztráció
        with ACTIVE_PROCESSES_LOCK:
            ACTIVE_PROCESSES.append(process)
        
        try:
            for line in process.stdout:
                if stop_event.is_set():
                    process.kill()
                    process.wait()
                    raise EncodingStopped()
                # FFmpeg kimenet kiírása a konzolba (ha van console_redirect beállítva)
                try:
                    import sys
                    if hasattr(sys.stdout, 'write'):
                        sys.stdout.write(line)
                        sys.stdout.flush()
                except (OSError, IOError, AttributeError):
                    pass
                if status_callback:
                    # Csak a frame= sorokat dolgozzuk fel progress számításhoz (figyelmeztető üzeneteket ignoráljuk)
                    if line.strip().startswith('frame='):
                        # Frame szám kinyerése
                        frame_match = re.search(r'frame=\s*(\d+)', line)
                        if frame_match and total_frames > 0:
                            current_frame = int(frame_match.group(1))
                            # Progress számítás frame alapján: current_frame / total_frames * duration
                            if video_fps > 0:
                                current_time = current_frame / video_fps
                            else:
                                # Fallback: ha nincs fps, akkor arányosan számolunk
                                current_time = (current_frame / total_frames) * duration_seconds if total_frames > 0 else 0
                            
                            # Korlátozzuk a videó hosszára
                            current_time = min(current_time, duration_seconds) if duration_seconds > 0 else current_time
                            
                            progress_hours = int(current_time // 3600)
                            progress_mins = int((current_time % 3600) // 60)
                            progress_secs = int(current_time % 60)
                            status_callback(f"{progress_hours:02d}:{progress_mins:02d}:{progress_secs:02d} / {duration_hours:02d}:{duration_mins:02d}:{duration_secs:02d}")
                        elif duration_seconds > 0:
                            # Fallback: ha nincs frame szám, akkor elapsed-et használjuk
                            elapsed_total = 0
                            if 'elapsed=' in line:
                                elapsed_match = re.search(r'elapsed=(\d+):(\d+):(\d+)(?:\.\d+)?', line)
                                if elapsed_match:
                                    elapsed_hours, elapsed_mins, elapsed_secs = map(int, elapsed_match.groups()[:3])
                                    elapsed_total = elapsed_hours * 3600 + elapsed_mins * 60 + elapsed_secs
                            
                            if elapsed_total > 0:
                                # Dinamikus becslés: az elapsed alapján, de korlátozzuk a videó hosszára
                                estimated_progress = min(elapsed_total * 2, duration_seconds)
                                progress_hours = int(estimated_progress // 3600)
                                progress_mins = int((estimated_progress % 3600) // 60)
                                progress_secs = int(estimated_progress % 60)
                                status_callback(f"{progress_hours:02d}:{progress_mins:02d}:{progress_secs:02d} / {duration_hours:02d}:{duration_mins:02d}:{duration_secs:02d}")
        except (OSError, IOError, BrokenPipeError, UnicodeDecodeError) as e:
            print(f"FFmpeg output olvasás hiba: {e}")
        
        try:
            process.wait()
        except (OSError, subprocess.SubprocessError) as e:
            print(f"FFmpeg wait hiba: {e}")
            if process.poll() is None:
                process.kill()
                process.wait()
        finally:
            # Process törlése a listából
            with ACTIVE_PROCESSES_LOCK:
                if process in ACTIVE_PROCESSES:
                    ACTIVE_PROCESSES.remove(process)
        
        if stop_event.is_set():
            raise EncodingStopped()

        success = process.returncode == 0
        
        debug_pause(
            f"FFmpeg kész: {'OK' if success else 'HIBA'} (CQ: {int(cq_value)})",
            "Validáció" if success else "Újrapróbálkozás",
            f"Output: {output_path}"
        )
        
        return success
    except EncodingStopped:
        raise
    except (subprocess.SubprocessError, OSError, ValueError, TypeError, AttributeError) as e:
        print(f"✗ Kódolási hiba: {e}")
        return False

def encode_video(input_path, output_path, initial_cq_value, subtitle_files, encoder='av1_nvenc', status_callback=None, initial_min_vmaf=None, vmaf_step=None, max_encoded_percent=None, stop_event=None, vmaf_value=None, resize_enabled=False, resize_height=1080, audio_compression_enabled=False, audio_compression_method='fast', svt_preset=2, logger=None):
    """Main video encoding workflow.
    
    Handles the entire encoding process including:
    - Audio stream analysis
    - CRF search (if needed)
    - Encoding execution
    - Validation
    - Retry logic with adjusted settings (VMAF fallback)
    
    Args:
        input_path: Path to input video.
        output_path: Path to output video.
        initial_cq_value: Initial CRF/CQ value.
        subtitle_files: List of subtitle files.
        encoder: Encoder name.
        status_callback: Callback for status updates.
        initial_min_vmaf: Target VMAF.
        vmaf_step: VMAF step size.
        max_encoded_percent: Max size percentage.
        stop_event: Event to stop process.
        vmaf_value: Current VMAF value.
        resize_enabled: Resize option.
        resize_height: Target height.
        audio_compression_enabled: Audio compression option.
        audio_compression_method: Audio compression method.
        svt_preset: SVT preset.
        logger: Logger instance.
        
    Returns:
        bool: True if successful, False otherwise.
    """
    if not input_path.exists():
        print(f"✗ Forrás fájl nem létezik: {input_path}")
        return False
    
    # KRITIKUS VÉDELEM: Fájl azonosítók mentése encoding ELŐTT
    # Ez védi meg, hogy ne keveredjenek a CRF értékek különböző videók között
    input_absolute_encode_start = input_path.absolute()
    try:
        input_stat_encode_start = input_path.stat()
        input_size_encode_start = input_stat_encode_start.st_size
        input_mtime_encode_start = input_stat_encode_start.st_mtime
    except (OSError, PermissionError) as e:
        print(f"✗ Encoding: nem sikerült a fájl stat() hívása: {input_path} - {e}")
        return False
    
    original_size = input_path.stat().st_size
    cq_value = initial_cq_value
    max_cq = 51 if encoder == 'av1_nvenc' else 63

    initial_min_vmaf, vmaf_step, max_encoded_percent = resolve_encoding_defaults(initial_min_vmaf, vmaf_step, max_encoded_percent)
    current_vmaf = initial_min_vmaf if vmaf_value is None else vmaf_value

    if stop_event is None:
        stop_event = STOP_EVENT
    
    # Hangsáv információk kiírása a CQ/CRF keresés előtt
    print(f"\n{'='*80}")
    print(f"🔊 HANGSÁVOK ELEMZÉSE: {input_path.name}")
    print(f"{'='*80}")
    
    try:
        default_lang, lang_51_count, lang_20_count = get_audio_streams_info(input_path)
        print(f"Alapértelmezett nyelv: {default_lang if default_lang else 'Nincs'}")
        
        if lang_51_count or lang_20_count:
            print(f"\nHangsávok nyelv szerint:")
            all_langs = set(list(lang_51_count.keys()) + list(lang_20_count.keys()))
            for lang in sorted(all_langs):
                count_51 = lang_51_count.get(lang, 0)
                count_20 = lang_20_count.get(lang, 0)
                lang_display = lang if lang != 'unknown' else 'Ismeretlen'
                if count_51 > 0:
                    print(f"  - {lang_display}: {count_51} db 5.1 hangsáv")
                if count_20 > 0:
                    print(f"  - {lang_display}: {count_20} db 2.0 hangsáv")
        else:
            print(f"Nincs hangsáv információ")
        
        # Hangdinamika kompresszió ellenőrzése
        if audio_compression_enabled:
            print(f"\n🔊 Hangdinamika kompresszió: BEKAPCSOLVA (módszer: {audio_compression_method})")
            if needs_audio_compression(input_path):
                if default_lang:
                    audio_51_stream_index = get_51_audio_stream_index(input_path, default_lang)
                    if audio_51_stream_index is not None:
                        print(f"  ✓ 5.1 hangsáv található az alapértelmezett nyelvhez (index: {audio_51_stream_index})")
                        print(f"  ✓ Új 2.0 hangsáv kerül hozzáadásra dinamika kompresszióval")
                    else:
                        print(f"  ✗ 5.1 hangsáv nem található az alapértelmezett nyelvhez")
                else:
                    print(f"  ✗ Alapértelmezett nyelv nem található")
            else:
                print(f"  ✗ Nem szükséges kompresszió (van 2.0 hangsáv vagy nincs 5.1)")
        else:
            print(f"\n🔊 Hangdinamika kompresszió: KIKAPCSOLVA")
        
        print(f"{'='*80}\n")
    except Exception as e:
        print(f"⚠ Hangsáv elemzés hiba: {e}\n")
    
    while cq_value <= max_cq:
        if stop_event.is_set():
            raise EncodingStopped()

        success = encode_single_attempt(input_path, output_path, cq_value, subtitle_files, encoder, status_callback, stop_event=stop_event, vmaf_value=current_vmaf, resize_enabled=resize_enabled, resize_height=resize_height, audio_compression_enabled=audio_compression_enabled, audio_compression_method=audio_compression_method, svt_preset=svt_preset, logger=logger)
        
        if not success:
            if output_path.exists() and not DEBUG_MODE:
                output_path.unlink()
            return False
        
        if not output_path.exists():
            return False
        
        new_size = output_path.stat().st_size
        
        if new_size < original_size:
            return True
        else:
            if current_vmaf > 85.0:
                current_vmaf_str = format_localized_number(current_vmaf, decimals=1)
                next_vmaf_str = format_localized_number(current_vmaf - vmaf_step, decimals=1)
                print(f"\n⚠ Fájl nagyobb, VMAF csökkentés: {current_vmaf_str} → {next_vmaf_str}")
                
                new_mb = new_size / (1024**2)
                orig_mb = original_size / (1024**2)
                new_mb_str = format_localized_number(new_mb, decimals=1)
                orig_mb_str = format_localized_number(orig_mb, decimals=1)
                debug_pause(
                    f"Fájl nagyobb ({new_mb_str} > {orig_mb_str} MB)",
                    f"VMAF csökkentés → új CRF keresés",
                    f"Fájl: {output_path}"
                )
                
                current_vmaf -= vmaf_step
                cq_result = run_crf_search(input_path, encoder, current_vmaf, vmaf_step, max_encoded_percent, stop_event=stop_event, svt_preset=svt_preset)
                
                # KRITIKUS VÉDELEM: Ellenőrzés a CRF keresés UTÁN
                # Biztosítjuk, hogy UGYANAZ a fájl van, mint az encoding kezdetekor
                if not input_path.exists():
                    raise FileNotFoundError(f"VÉGZETES HIBA: A forrás fájl ELTŰNT az encoding során!\n"
                                           f"Fájl: {input_absolute_encode_start}\n"
                                           f"Ez azt jelenti, hogy a CRF érték érvénytelen!")
                
                # Ellenőrzés: UGYANAZ az abszolút útvonal?
                input_absolute_encode_check = input_path.absolute()
                if input_absolute_encode_check != input_absolute_encode_start:
                    raise ValueError(f"VÉGZETES HIBA: A forrás fájl MEGVÁLTOZOTT az encoding során!\n"
                                   f"Encoding kezdetekor: {input_absolute_encode_start}\n"
                                   f"CRF keresés után: {input_absolute_encode_check}\n"
                                   f"Ez azt jelenti, hogy a CRF érték MÁS VIDEÓHOZ tartozik!\n"
                                   f"A program azonnal leáll a biztonság érdekében.")
                
                # Ellenőrzés: UGYANAZ a fájl méret és módosítási dátum?
                try:
                    input_stat_encode_check = input_path.stat()
                    input_size_encode_check = input_stat_encode_check.st_size
                    input_mtime_encode_check = input_stat_encode_check.st_mtime
                    
                    if input_size_encode_check != input_size_encode_start:
                        raise ValueError(f"VÉGZETES HIBA: A forrás fájl MÉRETE MEGVÁLTOZOTT az encoding során!\n"
                                       f"Méret kezdetkor: {input_size_encode_start:,} bytes\n"
                                       f"Méret CRF keresés után: {input_size_encode_check:,} bytes\n"
                                       f"Ez azt jelenti, hogy a fájl módosult, és a CRF érték érvénytelen!")
                    
                    if abs(input_mtime_encode_check - input_mtime_encode_start) > 1.0:
                        raise ValueError(f"VÉGZETES HIBA: A forrás fájl MÓDOSÍTÁSI DÁTUMA MEGVÁLTOZOTT az encoding során!\n"
                                       f"Dátum kezdetkor: {datetime.fromtimestamp(input_mtime_encode_start).strftime('%Y-%m-%d %H:%M:%S')}\n"
                                       f"Dátum CRF keresés után: {datetime.fromtimestamp(input_mtime_encode_check).strftime('%Y-%m-%d %H:%M:%S')}\n"
                                       f"Ez azt jelenti, hogy a fájl módosult, és a CRF érték érvénytelen!")
                except (OSError, PermissionError) as e:
                    raise FileNotFoundError(f"VÉGZETES HIBA: Nem sikerült ellenőrizni a fájlt a CRF keresés után: {e}")
                
                if isinstance(cq_result, tuple) and len(cq_result) == 3 and cq_result[2] and encoder == 'av1_nvenc':
                    raise NVENCFallbackRequired("NVENC VMAF fallback exhausted during encode_video()")
                new_cq = cq_result[0] if isinstance(cq_result, tuple) else cq_result
                
                if output_path.exists() and not DEBUG_MODE:
                    output_path.unlink()
                
                cq_value = new_cq
                continue
            
            if cq_value >= max_cq:
                if output_path.exists() and not DEBUG_MODE:
                    output_path.unlink()
                return False
            
            cq_value += 1
            
            if output_path.exists() and not DEBUG_MODE:
                output_path.unlink()

    return False

def remove_audio_track_from_file(source_path, audio_index, logger=None, stop_event=None):
    """Remove a specific audio track from the video file.
    
    Args:
        source_path: Path to the video file.
        audio_index: Index of the audio track to remove (0-based).
        logger: Logger instance.
        stop_event: Event to stop process.
        
    Returns:
        bool: True if successful, False otherwise.
    """

    if stop_event is None:
        stop_event = STOP_EVENT
    if stop_event.is_set():
        raise EncodingStopped()

    source_path = Path(source_path)
    source_suffix = source_path.suffix or '.mkv'
    safe_stem = source_path.stem or source_path.name
    temp_output = source_path.with_name(f"{safe_stem}.audioedit{source_suffix}")
    backup_path = source_path.with_name(source_path.name + ".audioedit.bak")

    if temp_output.exists():
        try:
            temp_output.unlink()
        except OSError:
            pass

    cmd = [
        FFMPEG_PATH,
        '-y',
        '-i', os.fspath(source_path),
        '-map', '0',
        '-c', 'copy',
        '-map', f'-0:a:{audio_index}',
        os.fspath(temp_output)
    ]

    if logger:
        logger.write(f"\n{'='*80}\n")
        logger.write(f"HANGSÁV ELTÁVOLÍTÁS: {source_path.name}\n")
        logger.write(f"PARANCS: {' '.join(cmd)}\n")
        logger.write(f"{'='*80}\n")
        logger.flush()

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True,
        startupinfo=get_startup_info()
    )

    with ACTIVE_PROCESSES_LOCK:
        ACTIVE_PROCESSES.append(process)

    try:
        for line in process.stdout:
            if stop_event.is_set():
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                raise EncodingStopped()
            if logger:
                logger.write(line)
        process.wait()
    finally:
        with ACTIVE_PROCESSES_LOCK:
            if process in ACTIVE_PROCESSES:
                ACTIVE_PROCESSES.remove(process)

    if process.returncode != 0:
        if temp_output.exists():
            temp_output.unlink()
        raise RuntimeError(f"FFmpeg hangsáv eltávolítás hiba (rc={process.returncode})")

    original_replaced = False
    try:
        if backup_path.exists():
            backup_path.unlink()
        source_path.replace(backup_path)
        original_replaced = True
        temp_output.replace(source_path)
    except (OSError, PermissionError, FileNotFoundError, shutil.Error) as e:
        # Fájl művelet hiba - próbáljuk meg visszaállítani az eredeti fájlt
        if original_replaced and backup_path.exists():
            try:
                backup_path.replace(source_path)
            except (OSError, PermissionError, FileNotFoundError, shutil.Error):
                # Ha a visszaállítás sem sikerül, logoljuk, de ne akadjon el
                pass
        if temp_output.exists():
            try:
                temp_output.unlink()
            except (OSError, PermissionError):
                pass
        raise
    finally:
        if backup_path.exists():
            try:
                backup_path.unlink()
            except OSError:
                pass
    if temp_output.exists():
        try:
            temp_output.unlink()
        except OSError:
            pass

    return True

def convert_audio_track_to_stereo(source_path, audio_index, method='fast', language_code=None, logger=None, stop_event=None):
    """Convert a specific audio track to stereo (2.0).
    
    Args:
        source_path: Path to the video file.
        audio_index: Index of the audio track to convert.
        method: Conversion method ('fast' or 'high_quality').
        language_code: Language code for metadata.
        logger: Logger instance.
        stop_event: Event to stop process.
        
    Returns:
        bool: True if successful, False otherwise.
    """

    if stop_event is None:
        stop_event = STOP_EVENT
    if stop_event.is_set():
        raise EncodingStopped()

    source_path = Path(source_path)
    source_suffix = source_path.suffix or '.mkv'
    safe_stem = source_path.stem or source_path.name
    temp_output = source_path.with_name(f"{safe_stem}.audioconv{source_suffix}")
    backup_path = source_path.with_name(source_path.name + ".audioconv.bak")

    if temp_output.exists():
        try:
            temp_output.unlink()
        except OSError:
            pass

    audio_details = get_audio_stream_details(source_path)
    new_audio_index = len(audio_details)
    filter_chain = build_audio_conversion_filter(method)
    title_text = get_audio_conversion_title(method)
    filter_label = f"stereo_{audio_index}"
    language_tag = (language_code or '').lower()

    cmd = [
        FFMPEG_PATH,
        '-y',
        '-i', os.fspath(source_path),
        '-map', '0',
        '-c', 'copy',
        '-map_metadata', '0',
        '-filter_complex', f"[0:a:{audio_index}]{filter_chain}[{filter_label}]",
        '-map', f'[{filter_label}]',
        f'-c:a:{new_audio_index}', 'aac',
        f'-b:a:{new_audio_index}', '192k',
        f'-ac:{new_audio_index}', '2',
        f'-metadata:s:a:{new_audio_index}', f'title={title_text}'
    ]

    if language_tag:
        cmd.extend([f'-metadata:s:a:{new_audio_index}', f'language={language_tag}'])

    if logger:
        logger.write(f"\n{'='*80}\n")
        logger.write(f"HANGSÁV 2.0 KONVERZIÓ: {source_path.name} (index: {audio_index}, mód: {method})\n")
        logger.write(f"PARANCS: {' '.join(cmd)}\n")
        logger.write(f"{'='*80}\n")
        logger.flush()

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True,
        startupinfo=get_startup_info()
    )

    with ACTIVE_PROCESSES_LOCK:
        ACTIVE_PROCESSES.append(process)

    try:
        for line in process.stdout:
            if stop_event.is_set():
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                raise EncodingStopped()
            if logger:
                logger.write(line)
        process.wait()
    finally:
        with ACTIVE_PROCESSES_LOCK:
            if process in ACTIVE_PROCESSES:
                ACTIVE_PROCESSES.remove(process)

    if process.returncode != 0:
        if temp_output.exists():
            temp_output.unlink()
        raise RuntimeError(f"FFmpeg konverzió hiba (rc={process.returncode})")

    original_replaced = False
    try:
        if backup_path.exists():
            backup_path.unlink()
        source_path.replace(backup_path)
        original_replaced = True
        temp_output.replace(source_path)
    except (OSError, PermissionError, FileNotFoundError, shutil.Error) as e:
        # Fájl művelet hiba - próbáljuk meg visszaállítani az eredeti fájlt
        if original_replaced and backup_path.exists():
            try:
                backup_path.replace(source_path)
            except (OSError, PermissionError, FileNotFoundError, shutil.Error):
                # Ha a visszaállítás sem sikerül, logoljuk, de ne akadjon el
                pass
        if temp_output.exists():
            try:
                temp_output.unlink()
            except (OSError, PermissionError):
                pass
        raise
    finally:
        if backup_path.exists():
            try:
                backup_path.unlink()
            except OSError:
                pass
        if temp_output.exists():
            try:
                temp_output.unlink()
            except OSError:
                pass

    return True

def open_video_file(file_path):
    try:
        if not file_path.exists():
            return False
        system = platform.system()
        if system == 'Windows':
            os.startfile(os.fspath(file_path))
        elif system == 'Darwin':
            subprocess.call(['open', os.fspath(file_path)])
        else:
            subprocess.call(['xdg-open', os.fspath(file_path)])
        return True
    except (OSError, subprocess.SubprocessError, FileNotFoundError):
        return False

class VideoEncoderGUI:
    def __init__(self, root):
        """Initialize the VideoEncoderGUI application.
        
        Args:
            root: The root Tkinter window.
        """
        global CURRENT_LANGUAGE
        CURRENT_LANGUAGE = get_default_language()
        
        self.root = root
        self.root.title(t('app_title'))
        self.root.geometry("1400x900")
        
        # Alapértelmezett méretek (reszponzív layout-hoz)
        self.default_entry_width_source = 50  # Forrás/Cél entry mezők
        self.default_entry_width_path = 38     # Path entry mezők
        self.default_slider_length = 200       # Csúszkák
        
        # Minimum méretek (alapértelmezett fele)
        self.min_entry_width_source = 25
        self.min_entry_width_path = 19
        self.min_slider_length = 100
        
        global GUI_INSTANCE
        GUI_INSTANCE = self

        # Program útvonalak automatikus észlelése
        detected_programs = auto_detect_programs()
        self.ffmpeg_path = tk.StringVar(value=detected_programs['ffmpeg'] or '')
        self.virtualdub_path = tk.StringVar(value=detected_programs['virtualdub'] or '')
        self.abav1_path = tk.StringVar(value=detected_programs['abav1'] or '')
        self.apply_tool_paths_from_gui()
        for var in (self.ffmpeg_path, self.virtualdub_path, self.abav1_path):
            var.trace_add('write', self._on_tool_path_change)
        
        # Összegzés kiírása
        if LOG_WRITER:
            try:
                LOG_WRITER.write("\n=== PROGRAM DETEKTÁLÁS ÖSSZEGZÉSE ===\n")
                LOG_WRITER.write(f"  FFmpeg: {'✓ ' + detected_programs['ffmpeg'] if detected_programs['ffmpeg'] else '✗ Nem található'}\n")
                LOG_WRITER.write(f"  VirtualDub2: {'✓ ' + detected_programs['virtualdub'] if detected_programs['virtualdub'] else '✗ Nem található'}\n")
                LOG_WRITER.write(f"  ab-av1: {'✓ ' + detected_programs['abav1'] if detected_programs['abav1'] else '✗ Nem található'}\n")
                LOG_WRITER.write("=====================================\n\n")
                LOG_WRITER.flush()
            except (OSError, IOError, AttributeError):
                pass

        self.source_path = None
        self.dest_path = None
        self.video_files = []
        self.video_items = {}
        self.subtitle_items = {}
        self.video_to_output = {}
        # Cache a betöltéskor kapott stat() értékekhez (hidegindítás optimalizáláshoz)
        # Struktúra: {video_path: {'source_size_bytes': int, 'source_modified_timestamp': float}}
        self.video_stat_cache = {}
        
        # Tree item mögötti eredeti adatok (gyors DB mentéshez, parse-olás nélkül)
        # Struktúra: {item_id: {'source_duration_seconds': float, 'source_frame_count': int, 'source_fps': float, ...}}
        self.tree_item_data = {}
        self.video_order = {}  # Sorszám tárolása: {video_path: order_number}
        
        self.sort_column = None  # Aktuális rendezési oszlop
        self.sort_reverse = False  # Csökkenő/növekvő rendezés
        self.encoding_start_times = {}  # {item_id: start_time} - átkódolás/VMAF kezdési időpont
        self.estimated_end_timer = None  # Timer a becsült befejezési idő frissítéséhez
        self.estimated_end_dates = {}  # Becsült befejezési idők tárolása (item_id -> dátum string)
        self.manual_nvenc_tasks = []  # Manuális NVENC újrakódolás taskok
        self.audio_edit_thread = None
        self.audio_edit_only_mode = False
        self.audio_edit_task_info = {}
        self.encoding_worker_running = False
        
        # Debounce timer a beállítások automatikus mentéséhez
        self.settings_save_timer = None
        
        # Adatbázis műveletek lock-ja - biztosítja, hogy az adatbázis műveletek ne ütközzenek
        self.db_lock = threading.Lock()
        self.db_thread_lock = threading.Lock()
        self.active_db_threads = []
        self.load_db_save_completed = threading.Event()  # Flag: betöltés utáni DB mentés befejeződött-e
        self.db_update_notification_timer = None  # Timer az update notification debounce-olásához
        self.manual_nvenc_active = False
        self.vmaf_worker_active = False
        self.nvenc_worker_threads = []
        self.nvenc_active_videos = set()
        self.nvenc_processing_videos = set()  # Videók, amelyek már az NVENC_QUEUE-ban vannak vagy feldolgozás alatt
        self.nvenc_selection_lock = threading.Lock()
        self.nvenc_worker_stats_lock = threading.Lock()
        self.nvenc_worker_stats = {'completed': 0, 'failed': 0, 'needs_check': 0}
        
        self.col_widths = {
            '#0': 40, 'video_name': 340, 'status': 200, 'cq': 40, 'vmaf': 40, 'psnr': 50, 'progress': 150,
            'orig_size': 70, 'new_size': 70, 'size_change': 50, 'duration': 80, 'frames': 80, 'completed_date': 120
        }
        
        # SQLite adatbázis útvonala (script mappájában) - már inicializálva _init_database-ben
        # Biztosítjuk, hogy mindig a script fájl tényleges mappájába mentse
        try:
            # __file__ abszolút útvonala
            script_file = Path(__file__).resolve()
            script_dir = script_file.parent
        except (OSError, ValueError, AttributeError):
            # Fallback: jelenlegi munkakönyvtár
            script_dir = Path.cwd()
        
        # SQLite adatbázis útvonala (script mappájában)
        self.db_path = script_dir / "save.db"
        # SQLite adatbázis inicializálása
        self._init_database()
        
        self.encoding_queue = queue.Queue()
        self.is_encoding = False
        self.copy_thread = None  # Nem-videó fájlok másolásához használt szál
        self.is_loading_videos = False
        self.auto_start_after_load = False  # Start gomb kérése esetén betöltés után automatikusan induljon-e
        self.last_load_errors = []
        self.current_video_index = -1
        self.graceful_stop_requested = False
        self.logged_invalid_subtitles = set()
        
        self.min_vmaf = tk.DoubleVar(value=97.0)
        self.vmaf_step = tk.DoubleVar(value=1.0)
        self.max_encoded_percent = tk.IntVar(value=75)
        self.svt_preset = tk.IntVar(value=2)
        self.debug_mode = tk.BooleanVar(value=False)
        self.auto_vmaf_psnr = tk.BooleanVar(value=False)
        self.resize_enabled = tk.BooleanVar(value=False)
        self.resize_height = tk.IntVar(value=1080)
        self.skip_av1_files = tk.BooleanVar(value=False)
        self.nvenc_worker_count = tk.IntVar(value=1)
        
        # Hangdinamika kompresszió
        self.audio_compression_enabled = tk.BooleanVar(value=False)
        self.audio_compression_method = tk.StringVar(value='fast')  # 'fast' vagy 'dialogue'
        
        # NVENC engedélyezés (40xx vagy 50xx GPU detektálása)
        nvenc_supported, gpu_name = detect_nvidia_gpu()
        self.nvenc_enabled = tk.BooleanVar(value=nvenc_supported)
        if gpu_name:
            self.detected_gpu_name = gpu_name
        else:
            self.detected_gpu_name = None
        default_nvenc_workers = 1
        if self.detected_gpu_name:
            gpu_name_upper = self.detected_gpu_name.upper()
            if "5090" in gpu_name_upper.replace(" ", ""):
                default_nvenc_workers = 3
        self.nvenc_worker_count.set(default_nvenc_workers)
        
        # Eredmény loggolása
        if LOG_WRITER:
            try:
                LOG_WRITER.write("=== NVENC ENGEDÉLYEZÉS EREDMÉNYE ===\n")
                if nvenc_supported and gpu_name:
                    LOG_WRITER.write(f"  ✓ NVENC engedélyezve\n")
                    LOG_WRITER.write(f"  GPU: {gpu_name}\n")
                else:
                    LOG_WRITER.write(f"  ✗ NVENC nincs engedélyezve\n")
                    if gpu_name:
                        LOG_WRITER.write(f"  GPU: {gpu_name} (nem 40xx/50xx sorozat)\n")
                    else:
                        LOG_WRITER.write(f"  GPU: Nem található vagy nem NVIDIA\n")
                LOG_WRITER.write("=====================================\n\n")
                LOG_WRITER.flush()
            except (OSError, IOError, AttributeError):
                pass
        self.current_min_vmaf = float(self.min_vmaf.get())
        self.current_vmaf_step = float(self.vmaf_step.get())
        self.current_max_encoded_percent = int(self.max_encoded_percent.get())
        
        # Console logger-ek
        self.tree = None
        self.nvenc_logger = None
        self.svt_logger = None
        self.nvenc_loggers = []
        self.nvenc_consoles = []
        self.nvenc_log_files = []
        
        # Elkészültek elrejtése checkbox
        self.hide_completed = tk.BooleanVar(value=False)
        
        self.setup_ui()
        
        # Reszponzív layout: ablak resize esemény beállítása
        self.root.bind('<Configure>', self.on_window_resize)
        
        # Állapot betöltés ellenőrzése és felajánlása program induláskor
        self.root.after(100, self.check_and_offer_state_load)
        
    def check_and_offer_state_load(self):
        """Check for saved database state and offer to load it.
        
        If a saved state exists in the database, prompts the user to load it.
        """

        # Ellenőrizzük, hogy a tree és a source_entry is inicializálva van-e
        if not hasattr(self, 'tree') or self.tree is None or not hasattr(self, 'source_entry') or self.source_entry is None:
            self.root.after(100, self.check_and_offer_state_load)
            return
        saved_state = self.load_state_from_db()
        
        if saved_state:
            saved_source = saved_state.get('source_path')
            saved_dest = saved_state.get('dest_path')
            
            if saved_source:
                result = messagebox.askyesno(
                    "Előző állapot betöltése",
                    f"Található előző mentett állapot!\n\nForrás: {saved_source}\nCél: {saved_dest or 'Nincs'}\n\nBetöltöd az előző állapotot?\n\n(A forrás és cél mappák automatikusan be lesznek állítva)"
                )
                
                if result:
                    # Forrás mappa beállítása
                    if saved_source:
                        self.source_entry.delete(0, tk.END)
                        self.source_entry.insert(0, saved_source)
                    
                    # Cél mappa beállítása
                    if saved_dest:
                        self.dest_entry.delete(0, tk.END)
                        self.dest_entry.insert(0, saved_dest)
                    
                    # VMAF beállítások visszaállítása
                    if 'min_vmaf' in saved_state:
                        self.update_vmaf_label(saved_state['min_vmaf'])
                    if 'vmaf_step' in saved_state:
                        self.update_vmaf_step_label(saved_state['vmaf_step'])
                    if 'max_encoded_percent' in saved_state:
                        self.update_max_encoded_label(saved_state['max_encoded_percent'])
                    if 'resize_enabled' in saved_state:
                        self.resize_enabled.set(saved_state['resize_enabled'])
                        self.toggle_resize_slider()
                    if 'resize_height' in saved_state:
                        self.update_resize_label(saved_state['resize_height'])
                    if 'auto_vmaf_psnr' in saved_state:
                        self.auto_vmaf_psnr.set(saved_state['auto_vmaf_psnr'])
                    if 'nvenc_worker_count' in saved_state:
                        self.nvenc_worker_count.set(int(saved_state['nvenc_worker_count']))
                        self.update_nvenc_workers_label(saved_state['nvenc_worker_count'])
                    if 'svt_preset' in saved_state:
                        svt_preset_val = int(saved_state['svt_preset']) if saved_state['svt_preset'] else 2
                        self.svt_preset.set(svt_preset_val)
                        self.svt_preset_value_label.config(text=str(svt_preset_val))
                    
                    # Videók betöltése az állapottal
                    self.load_videos()
        
    def setup_ui(self):
        """Initialize and arrange the User Interface components.
        
        Sets up the main window layout, including:
        - Top control panel (language, paths, settings)
        - Video list Treeview
        - Bottom control panel (buttons, status, progress)
        """
        top_frame = ttk.Frame(self.root, padding="10")
        top_frame.pack(fill=tk.X)
        
        # Egyenletes sorok közötti távolság beállítása a top_frame-ben
        for i in range(7):
            top_frame.grid_rowconfigure(i, uniform='rows', weight=1)
        
        # Nyelvválasztó jobb felső sarokban (kb 1cm = 37px offset a tallózás gombtól)
        lang_frame = ttk.Frame(top_frame)
        lang_frame.grid(row=0, column=4, rowspan=7, sticky=tk.N, padx=(37, 5), pady=5)
        
        # Egyenletes sorok közötti távolság beállítása a lang_frame-ben is
        for i in range(7):
            lang_frame.grid_rowconfigure(i, uniform='rows', weight=1)
        
        self.language_label = ttk.Label(lang_frame, text=t('language'), width=18, anchor=tk.W)
        self.language_label.grid(row=0, column=0, sticky=tk.W, padx=(0, 2), pady=(0, 0))
        self.language_var = tk.StringVar()
        lang_display = {'hu': t('hungarian'), 'en': t('english')}
        self.lang_combo = ttk.Combobox(lang_frame, textvariable=self.language_var, 
                                      values=[lang_display['hu'], lang_display['en']], 
                                      state='readonly', width=15)
        self.lang_combo.grid(row=0, column=1, sticky=tk.W, padx=2, pady=(0, 0))
        self.lang_combo.bind('<<ComboboxSelected>>', self.change_language)
        
        # Nyelv megjelenítése
        self.lang_combo.set(lang_display.get(CURRENT_LANGUAGE, CURRENT_LANGUAGE))
        
        # FFmpeg útvonal
        self.ffmpeg_label = ttk.Label(lang_frame, text=t('ffmpeg_path'), width=18, anchor=tk.W)
        self.ffmpeg_label.grid(row=1, column=0, sticky=tk.W, padx=(0, 2), pady=(0, 0))
        self.ffmpeg_entry = ttk.Entry(lang_frame, textvariable=self.ffmpeg_path, width=38)
        self.ffmpeg_entry.grid(row=1, column=1, sticky=tk.W, padx=2, pady=(0, 0))
        self.ffmpeg_browse_btn = ttk.Button(lang_frame, text=t('browse'), command=self.browse_ffmpeg)
        self.ffmpeg_browse_btn.grid(row=1, column=2, padx=2, pady=(0, 0))
        
        # VirtualDub2 útvonal
        self.vdub_label = ttk.Label(lang_frame, text=t('virtualdub_path'), width=18, anchor=tk.W)
        self.vdub_label.grid(row=2, column=0, sticky=tk.W, padx=(0, 2), pady=(5, 5))
        self.vdub_entry = ttk.Entry(lang_frame, textvariable=self.virtualdub_path, width=38)
        self.vdub_entry.grid(row=2, column=1, sticky=tk.W, padx=2, pady=(5, 5))
        self.vdub_browse_btn = ttk.Button(lang_frame, text=t('browse'), command=self.browse_virtualdub)
        self.vdub_browse_btn.grid(row=2, column=2, padx=2, pady=(5, 5))
        
        # ab-av1 útvonal
        self.abav1_label = ttk.Label(lang_frame, text=t('abav1_path'), width=18, anchor=tk.W)
        self.abav1_label.grid(row=3, column=0, sticky=tk.W, padx=(0, 2), pady=(5, 5))
        self.abav1_entry = ttk.Entry(lang_frame, textvariable=self.abav1_path, width=38)
        self.abav1_entry.grid(row=3, column=1, sticky=tk.W, padx=2, pady=(5, 5))
        self.abav1_browse_btn = ttk.Button(lang_frame, text=t('browse'), command=self.browse_abav1)
        self.abav1_browse_btn.grid(row=3, column=2, padx=2, pady=(5, 5))
        
        # NVENC engedélyezés checkbox
        nvenc_text = t('nvenc_enabled')
        if self.detected_gpu_name:
            nvenc_text += f" ({self.detected_gpu_name})"
        self.nvenc_checkbutton = ttk.Checkbutton(
            lang_frame,
            text=nvenc_text,
            variable=self.nvenc_enabled
        )
        self.nvenc_checkbutton.grid(row=4, column=0, columnspan=3, sticky=tk.W, padx=(0, 2), pady=(5, 5))
        self.nvenc_enabled.trace_add('write', self._on_nvenc_toggle)
        self._update_nvenc_checkbox_text()
        
        # SVT-AV1 preset csúszka
        svt_preset_frame = ttk.Frame(lang_frame)
        svt_preset_frame.grid(row=5, column=0, columnspan=3, sticky=tk.W, padx=(0, 2), pady=(5, 5))
        
        self.svt_preset_label = ttk.Label(svt_preset_frame, text='SVT-AV1 Preset:', width=18, anchor=tk.W)
        self.svt_preset_label.pack(side=tk.LEFT, padx=(0, 2))
        
        self.svt_preset_slider = ttk.Scale(
            svt_preset_frame,
            from_=1,
            to=6,
            orient=tk.HORIZONTAL,
            variable=self.svt_preset,
            length=150,
            command=self.update_svt_preset_label
        )
        self.svt_preset_slider.pack(side=tk.LEFT, padx=2)
        
        self.svt_preset_value_label = ttk.Label(svt_preset_frame, text="2", font=("Arial", 10, "bold"))
        self.svt_preset_value_label.pack(side=tk.LEFT, padx=2)

        # NVENC worker count slider (SVT preset alatt)
        nvenc_workers_frame = ttk.Frame(lang_frame)
        nvenc_workers_frame.grid(row=6, column=0, columnspan=3, sticky=tk.W, padx=(0, 2), pady=(5, 5))

        self.nvenc_workers_label = ttk.Label(nvenc_workers_frame, text=t('nvenc_workers'), width=18, anchor=tk.W)
        self.nvenc_workers_label.pack(side=tk.LEFT, padx=(0, 2))

        self.nvenc_workers_slider = ttk.Scale(
            nvenc_workers_frame,
            from_=1,
            to=3,
            orient=tk.HORIZONTAL,
            variable=self.nvenc_worker_count,
            length=150,
            command=self.update_nvenc_workers_label
        )
        try:
            self.nvenc_workers_slider.configure(resolution=1)
        except (tk.TclError, AttributeError):
            pass
        self.nvenc_workers_slider.pack(side=tk.LEFT, padx=2)

        self.nvenc_workers_value_label = ttk.Label(
            nvenc_workers_frame,
            text=str(int(self.nvenc_worker_count.get())),
            font=("Arial", 10, "bold")
        )
        self.nvenc_workers_value_label.pack(side=tk.LEFT, padx=2)
        self.update_nvenc_workers_label(self.nvenc_worker_count.get())
        
        # Bal oldal: Forrás, Cél, Debug, Videók betöltése
        # Címkék fix szélességgel, hogy ne változzon a layout nyelvváltáskor
        self.source_label = ttk.Label(top_frame, text=t('source'), width=12, anchor=tk.W)
        self.source_label.grid(row=0, column=0, sticky=tk.W, padx=5)
        self.source_entry = ttk.Entry(top_frame, width=50)
        self.source_entry.grid(row=0, column=1, padx=5)
        self.source_browse_btn = ttk.Button(top_frame, text=t('browse'), command=self.browse_source)
        self.source_browse_btn.grid(row=0, column=2, padx=5)
        
        self.dest_label = ttk.Label(top_frame, text=t('dest'), width=12, anchor=tk.W)
        self.dest_label.grid(row=1, column=0, sticky=tk.W, padx=5)
        self.dest_entry = ttk.Entry(top_frame, width=50)
        self.dest_entry.grid(row=1, column=1, padx=5)
        self.dest_browse_btn = ttk.Button(top_frame, text=t('browse'), command=self.browse_dest)
        self.dest_browse_btn.grid(row=1, column=2, padx=5)
        
        # Debug checkbox bal oldalon
        self.debug_checkbutton = ttk.Checkbutton(
            top_frame,
            text=t('debug_mode'),
            variable=self.debug_mode,
            command=self.toggle_debug_mode
        )
        self.debug_checkbutton.grid(row=2, column=0, columnspan=3, sticky=tk.W, padx=5, pady=5)
        
        # Automatikus VMAF/PSNR számítás checkbox
        self.auto_vmaf_psnr_checkbutton = ttk.Checkbutton(
            top_frame,
            text=t('auto_vmaf_psnr'),
            variable=self.auto_vmaf_psnr,
            command=self._save_settings_debounced
        )
        self.auto_vmaf_psnr_checkbutton.grid(row=3, column=0, columnspan=3, sticky=tk.W, padx=5, pady=5)
        
        # Videók betöltése gomb bal oldalon
        self.load_videos_btn = ttk.Button(top_frame, text=t('load_videos'), command=self.load_videos)
        self.load_videos_btn.grid(row=4, column=0, columnspan=3, sticky=tk.W, padx=5, pady=5)
        
        # Jobb oldal: Csúszkák (kb 1cm = 37px offset a tallózás gombtól)
        # Min VMAF csúszka jobbra
        vmaf_frame = ttk.Frame(top_frame)
        vmaf_frame.grid(row=0, column=3, padx=(37, 5), pady=0, sticky=tk.W+tk.N+tk.S)
        
        self.min_vmaf_label = ttk.Label(vmaf_frame, text=t('min_vmaf'), width=20, anchor=tk.W)
        self.min_vmaf_label.pack(side=tk.LEFT, padx=5)
        
        self.vmaf_slider = ttk.Scale(
            vmaf_frame,
            from_=85.0,
            to=99.9,
            orient=tk.HORIZONTAL,
            variable=self.min_vmaf,
            length=200,
            command=self.update_vmaf_label
        )
        try:
            self.vmaf_slider.configure(resolution=0.5)
        except (tk.TclError, AttributeError):
            pass
        
        self.vmaf_slider.pack(side=tk.LEFT, padx=5)
        
        self.vmaf_value_label = ttk.Label(vmaf_frame, text="97.0", font=("Arial", 10, "bold"))
        self.vmaf_value_label.pack(side=tk.LEFT, padx=5)
        
        # VMAF Fallback csúszka jobbra
        vmaf_step_frame = ttk.Frame(top_frame)
        vmaf_step_frame.grid(row=1, column=3, padx=(37, 5), pady=0, sticky=tk.W+tk.N+tk.S)
        
        self.vmaf_fallback_label = ttk.Label(vmaf_step_frame, text=t('vmaf_fallback'), width=20, anchor=tk.W)
        self.vmaf_fallback_label.pack(side=tk.LEFT, padx=5)
        
        self.vmaf_step_slider = ttk.Scale(
            vmaf_step_frame,
            from_=0.5,
            to=5.0,
            orient=tk.HORIZONTAL,
            variable=self.vmaf_step,
            length=200,
            command=self.update_vmaf_step_label
        )
        try:
            self.vmaf_step_slider.configure(resolution=0.1)
        except (tk.TclError, AttributeError):
            pass
        
        self.vmaf_step_slider.pack(side=tk.LEFT, padx=5)
        
        self.vmaf_step_value_label = ttk.Label(vmaf_step_frame, text="1.0", font=("Arial", 10, "bold"))
        self.vmaf_step_value_label.pack(side=tk.LEFT, padx=5)
        
        # Max Encoded csúszka jobbra
        max_encoded_frame = ttk.Frame(top_frame)
        max_encoded_frame.grid(row=2, column=3, padx=(37, 5), pady=0, sticky=tk.W+tk.N+tk.S)
        
        self.max_encoded_label = ttk.Label(max_encoded_frame, text=t('max_encoded'), width=20, anchor=tk.W)
        self.max_encoded_label.pack(side=tk.LEFT, padx=5)
        
        self.max_encoded_slider = ttk.Scale(
            max_encoded_frame,
            from_=1,
            to=100,
            orient=tk.HORIZONTAL,
            variable=self.max_encoded_percent,
            length=200,
            command=self.update_max_encoded_label
        )
        self.max_encoded_slider.pack(side=tk.LEFT, padx=5)
        
        self.max_encoded_value_label = ttk.Label(max_encoded_frame, text="75%", font=("Arial", 10, "bold"))
        self.max_encoded_value_label.pack(side=tk.LEFT, padx=5)
        
        # Resize (Átméretezés) checkbox és csúszka
        resize_frame = ttk.Frame(top_frame)
        resize_frame.grid(row=3, column=3, padx=(37, 5), pady=0, sticky=tk.W+tk.N+tk.S)
        
        self.resize_checkbox = ttk.Checkbutton(
            resize_frame,
            text=t('resize_height'),
            variable=self.resize_enabled,
            command=lambda: (self.toggle_resize_slider(), self._save_settings_debounced())
        )
        self.resize_checkbox.pack(side=tk.LEFT, padx=5)
        
        self.resize_slider = ttk.Scale(
            resize_frame,
            from_=360,
            to=2160,
            orient=tk.HORIZONTAL,
            variable=self.resize_height,
            length=200,
            command=self.update_resize_label
        )
        try:
            self.resize_slider.configure(resolution=10)
        except (tk.TclError, AttributeError):
            pass
        
        self.resize_slider.pack(side=tk.LEFT, padx=5)
        
        self.resize_value_label = ttk.Label(resize_frame, text="1080p", font=("Arial", 10, "bold"))
        self.resize_value_label.pack(side=tk.LEFT, padx=5)

        # Kezdetben elrejtjük a csúszkát
        self.resize_slider.pack_forget()
        self.resize_value_label.pack_forget()
        
        # Skip AV1 fájlok checkbox a resize height alatt
        skip_av1_frame = ttk.Frame(top_frame)
        skip_av1_frame.grid(row=4, column=3, padx=(37, 5), pady=0, sticky=tk.W+tk.N+tk.S)
        
        self.skip_av1_checkbutton = ttk.Checkbutton(
            skip_av1_frame,
            text=t('skip_av1'),
            variable=self.skip_av1_files
        )
        self.skip_av1_checkbutton.pack(side=tk.LEFT, padx=5)
        
        # Hangdinamika kompresszió checkbox és módszer választó
        audio_compression_frame = ttk.Frame(top_frame)
        audio_compression_frame.grid(row=5, column=3, padx=(37, 5), pady=0, sticky=tk.W+tk.N+tk.S)
        
        self.audio_compression_checkbutton = ttk.Checkbutton(
            audio_compression_frame,
            text=t('audio_compression'),
            variable=self.audio_compression_enabled,
            command=self._save_settings_debounced
        )
        self.audio_compression_checkbutton.pack(side=tk.LEFT, padx=5)
        
        self.audio_compression_combo = ttk.Combobox(
            audio_compression_frame,
            values=[t('audio_compression_fast'), t('audio_compression_dialogue')],
            state='readonly',
            width=20
        )
        self.audio_compression_combo.pack(side=tk.LEFT, padx=5)
        self.audio_compression_combo.set(t('audio_compression_fast'))
        # Event handler a combobox változásához
        def on_audio_method_change(event=None):
            selected = self.audio_compression_combo.get()
            if selected == t('audio_compression_fast'):
                self.audio_compression_method.set('fast')
            elif selected == t('audio_compression_dialogue'):
                self.audio_compression_method.set('dialogue')
            self._save_settings_debounced()  # Automatikus mentés debounce-szal
        self.audio_compression_combo.bind('<<ComboboxSelected>>', on_audio_method_change)
        # Kezdeti érték beállítása
        self.audio_compression_method.set('fast')
        
        # === NOTEBOOK (több fül) ===
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 5))
        
        # FÜL 1: Videók (TreeView)
        self.videos_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.videos_tab, text=t('videos_tab'))
        
        # NVENC konzol fülek (top-level, dinamikus megjelenítéssel)
        self.max_nvenc_consoles = 3
        self.nvenc_console_frames = []
        self.nvenc_consoles = []
        self.nvenc_loggers = []
        self.nvenc_log_files = []

        # NVENC és SVT-AV1 log fájlok létrehozása (törlés és újra létrehozás)
        try:
            script_file = Path(__file__).resolve()
            script_dir = script_file.parent
        except (OSError, ValueError, AttributeError):
            script_dir = Path.cwd()
        
        nvenc_log_paths = [script_dir / f"nvenc_console_{idx + 1}.log" for idx in range(self.max_nvenc_consoles)]
        svt_log_path = script_dir / "svt_console.log"

        # Log fájlok törlése, ha léteznek
        for nvenc_log_path in nvenc_log_paths:
            if nvenc_log_path.exists():
                try:
                    nvenc_log_path.unlink()
                except (OSError, PermissionError, FileNotFoundError):
                    pass

        if svt_log_path.exists():
            try:
                svt_log_path.unlink()
            except (OSError, PermissionError, FileNotFoundError):
                pass

        # Log fájlok létrehozása
        for idx, nvenc_log_path in enumerate(nvenc_log_paths):
            try:
                log_file = open(nvenc_log_path, "w", encoding="utf-8")
                log_file.write(f"=== NVENC KONZOL LOG #{idx + 1} ===\n")
                log_file.write(f"Indítás: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                log_file.write(f"{'='*80}\n\n")
                log_file.flush()
            except (OSError, IOError, PermissionError):
                log_file = None
            self.nvenc_log_files.append(log_file)

        try:
            svt_log_file = open(svt_log_path, "w", encoding="utf-8")
            svt_log_file.write(f"=== SVT-AV1 KONZOL LOG ===\n")
            svt_log_file.write(f"Indítás: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            svt_log_file.write(f"{'='*80}\n\n")
            svt_log_file.flush()
        except (OSError, IOError, PermissionError):
            svt_log_file = None

        # NVENC konzol tabok és loggerek létrehozása
        for idx in range(self.max_nvenc_consoles):
            frame = ttk.Frame(self.notebook)
            self.notebook.add(frame, text=f"{t('nvenc_console')} {idx + 1}")
            console_widget = scrolledtext.ScrolledText(
                frame,
                wrap=tk.WORD,
                width=120,
                height=30,
                font=("Consolas", 9),
                bg="#1e1e1e",
                fg="#d4d4d4",
                state=tk.DISABLED
            )
            console_widget.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
            self.nvenc_console_frames.append(frame)
            self.nvenc_consoles.append(console_widget)
            log_file = self.nvenc_log_files[idx] if idx < len(self.nvenc_log_files) else None
            # Átadjuk a log_files_list-et is, és a logger_index-et is, hogy a logger_index alapján választhassa a fájlt
            # A logger_index nem változik, így elkerüljük a race condition-t
            nvenc_logger = ConsoleLogger(console_widget, self.encoding_queue, log_file=log_file, log_files_list=self.nvenc_log_files, logger_index=idx)
            nvenc_logger.set_encoder_type('nvenc')
            nvenc_logger.set_worker_index(idx)  # Kezdetben ugyanaz, mint a logger_index
            self.nvenc_loggers.append(nvenc_logger)
        
        if self.nvenc_consoles:
            self.nvenc_console = self.nvenc_consoles[0]
        else:
            self.nvenc_console = None
        if self.nvenc_loggers:
            self.nvenc_logger = self.nvenc_loggers[0]
        else:
            self.nvenc_logger = None

        self.refresh_nvenc_console_tabs(self.get_configured_nvenc_workers())
        
        # FÜL: SVT-AV1 Konzol
        self.svt_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.svt_tab, text=t('svt_console'))
        
        self.svt_console = scrolledtext.ScrolledText(
            self.svt_tab,
            wrap=tk.WORD,
            width=120,
            height=30,
            font=("Consolas", 9),
            bg="#1e1e1e",
            fg="#d4d4d4",
            state=tk.DISABLED
        )
        self.svt_console.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # SVT logger létrehozása log fájllal
        self.svt_logger = ConsoleLogger(self.svt_console, self.encoding_queue, log_file=svt_log_file)
        self.svt_logger.set_encoder_type('svt')
        
        # Log fájlok tárolása a bezáráshoz
        self.svt_log_file = svt_log_file

        # Alkalmazás bezárásakor log fájlok bezárása
        def on_closing():
            if hasattr(self, 'nvenc_log_files'):
                for log_file in self.nvenc_log_files:
                    if log_file:
                        try:
                            log_file.close()
                        except (OSError, IOError, AttributeError):
                            pass
            if hasattr(self, 'svt_log_file') and self.svt_log_file:
                try:
                    self.svt_log_file.close()
                except (OSError, IOError, AttributeError):
                    pass
            try:
                self._wait_for_db_threads()
            except Exception:
                pass
            self.root.destroy()
        
        self.root.protocol("WM_DELETE_WINDOW", on_closing)
        
        # TreeView inicializálása
        self._setup_treeview()

    def _refresh_nvenc_console_tab_titles(self):
        if hasattr(self, 'nvenc_console_frames'):
            for idx, frame in enumerate(self.nvenc_console_frames):
                try:
                    self.notebook.tab(frame, text=f"{t('nvenc_console')} {idx + 1}")
                except tk.TclError:
                    continue

    def refresh_nvenc_console_tabs(self, desired_count):
        if not hasattr(self, 'nvenc_console_frames'):
            return
        try:
            desired = int(desired_count)
        except (ValueError, TypeError, tk.TclError):
            desired = 1
        desired = max(1, min(self.max_nvenc_consoles, desired))
        self.current_nvenc_console_count = desired
        for idx, frame in enumerate(self.nvenc_console_frames):
            state = 'normal' if idx < desired else 'hidden'
            try:
                self.notebook.tab(frame, state=state)
                self.notebook.tab(frame, text=f"{t('nvenc_console')} {idx + 1}")
            except tk.TclError:
                continue

    def get_configured_nvenc_workers(self):
        try:
            workers = int(self.nvenc_worker_count.get())
        except (ValueError, TypeError, tk.TclError):
            workers = 1
        return max(1, min(self.max_nvenc_consoles if hasattr(self, 'max_nvenc_consoles') else 3, workers))

    def _setup_treeview(self):
        """Initialize the TreeView widget.
        
        Sets up columns, headings, and scrollbars for the video list.
        """
        # TreeView áthelyezése az első fülre
        tree_frame = ttk.Frame(self.videos_tab, padding="10")
        tree_frame.pack(fill=tk.BOTH, expand=True)
        
        columns = ("video_name", "status", "cq", "vmaf", "psnr", "progress", "orig_size", "new_size", "size_change", "duration", "frames", "completed_date")
        # Oszlop index táblázat - így nem kell ezer helyen javítani, ha módosítás történik
        self.COLUMN_INDEX = {col: idx for idx, col in enumerate(columns)}
        self.tree = ttk.Treeview(tree_frame, columns=columns, show="tree headings", height=15, displaycolumns=columns)
        
        self.tree.heading("#0", text=t('column_order'), command=lambda: self.sort_by_column("#0"))
        self.tree.heading("video_name", text=t('column_video'), command=lambda: self.sort_by_column("video_name"))
        self.tree.heading("status", text=t('column_status'), command=lambda: self.sort_by_column("status"))
        self.tree.heading("cq", text=t('column_cq'), command=lambda: self.sort_by_column("cq"))
        self.tree.heading("vmaf", text=t('column_vmaf'), command=lambda: self.sort_by_column("vmaf"))
        self.tree.heading("psnr", text=t('column_psnr'), command=lambda: self.sort_by_column("psnr"))
        self.tree.heading("progress", text=t('column_progress'), command=lambda: self.sort_by_column("progress"))
        self.tree.heading("orig_size", text=t('column_orig_size'), command=lambda: self.sort_by_column("orig_size"))
        self.tree.heading("new_size", text=t('column_new_size'), command=lambda: self.sort_by_column("new_size"))
        self.tree.heading("size_change", text=t('column_size_change'), command=lambda: self.sort_by_column("size_change"))
        self.tree.heading("duration", text="Időtartam", command=lambda: self.sort_by_column("duration"))
        self.tree.heading("frames", text="Frame-ek", command=lambda: self.sort_by_column("frames"))
        self.tree.heading("completed_date", text=t('column_completed'), command=lambda: self.sort_by_column("completed_date"))
        
        for col, width in self.col_widths.items():
            # Balra igazítás a fájlméret oszlopoknál (számok könnyebb olvashatósága)
            if col in ("orig_size", "new_size", "size_change", "duration", "frames"):
                self.tree.column(col, width=width, anchor=tk.W)
            elif col in ("vmaf", "psnr", "cq"):
                # VMAF, PSNR és CQ oszlopok is balra igazítva (számok)
                self.tree.column(col, width=width, anchor=tk.W)
            else:
                self.tree.column(col, width=width)
        
        self.tree.bind('<B1-Motion>', self.on_column_resize)
        self.tree.bind('<Double-Button-1>', self.on_double_click)
        self.tree.bind('<Button-3>', self.on_right_click)
        
        scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.summary_frame = ttk.Frame(self.videos_tab, padding="10")
        # Alapértelmezetten elrejtjük, csak akkor mutatjuk, ha van összesítés
        # self.summary_frame.pack(fill=tk.X)
        
        self.summary_tree = ttk.Treeview(self.summary_frame, columns=columns, show="tree", height=1, displaycolumns=columns)
        for col, width in self.col_widths.items():
            try:
                # Balra igazítás a fájlméret oszlopoknál (számok könnyebb olvashatósága)
                if col in ("orig_size", "new_size", "size_change"):
                    self.summary_tree.column(col, width=width, anchor=tk.W)
                else:
                    self.summary_tree.column(col, width=width)
            except (tk.TclError, AttributeError):
                pass
        self.summary_tree.pack(fill=tk.X)
        
        self.tree.tag_configure("pending", foreground="blue")
        self.tree.tag_configure("encoding", foreground="orange")
        self.tree.tag_configure("encoding_nvenc", foreground="orange")
        self.tree.tag_configure("encoding_svt", foreground="purple")
        self.tree.tag_configure("audio_edit", foreground="#008080")
        self.tree.tag_configure("completed", foreground="green")
        self.tree.tag_configure("needs_check", foreground="darkorange")
        self.tree.tag_configure("failed", foreground="red")
        self.tree.tag_configure("subtitle", foreground="gray")
        self.summary_tree.tag_configure("summary", background="#e8e8e8", font=("Arial", 10, "bold"))
        
        bottom_frame = ttk.Frame(self.root, padding="10")
        bottom_frame.pack(fill=tk.X, side=tk.BOTTOM)
        
        button_frame = ttk.Frame(bottom_frame)
        button_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.start_button = ttk.Button(button_frame, text=t('btn_start'), command=self.start_encoding)
        self.start_button.pack(side=tk.LEFT, padx=5)
        
        # stop_button eltávolítva - a start_button fog "Leállítás" gombként működni futás közben
        self.stop_button = None  # Nincs többé külön stop gomb

        self.immediate_stop_button = ttk.Button(button_frame, text=t('btn_immediate_stop'), command=self.stop_encoding_immediate, state=tk.DISABLED)
        self.immediate_stop_button.pack(side=tk.LEFT, padx=5)
        
        self.clear_table_btn = ttk.Button(button_frame, text=t('btn_clear_table'), command=self.clear_table)
        self.clear_table_btn.pack(side=tk.LEFT, padx=5)
        
        self.hide_completed_checkbutton = ttk.Checkbutton(
            button_frame,
            text=t('btn_hide_completed'),
            variable=self.hide_completed,
            command=self.toggle_hide_completed
        )
        self.hide_completed_checkbutton.pack(side=tk.LEFT, padx=5)
        
        # Elrejtett item_id-k tárolása (csak megjelenítéshez, adatbázis mentéshez nem kell)
        
        # Timer indítása a becsült befejezési idő frissítéséhez (10 másodpercenként)
        self.start_estimated_end_timer()
        self.hidden_items = set()
        
        # Status sor Frame (status_label bal oldalon, notification jobb oldalon)
        status_frame = ttk.Frame(bottom_frame)
        status_frame.pack(fill=tk.X)
        
        self.status_label = ttk.Label(status_frame, text=t('status_ready'), font=("Arial", 11, "bold"))
        self.status_label.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        # Notification label (Adatbázis mentés jelzéshez)
        self.db_notification_label = ttk.Label(status_frame, text="", font=("Arial", 10), foreground="green")
        self.db_notification_label.pack(side=tk.RIGHT, padx=10)
        
        self.progress_bar = ttk.Progressbar(bottom_frame, mode='determinate')
        self.progress_bar.pack(fill=tk.X, pady=5)
    
    def toggle_debug_mode(self):
        global DEBUG_MODE
        DEBUG_MODE = self.debug_mode.get()
        # Debug mód átváltva (naplózás kikommentezve)
    
    def show_db_notification(self):
        """Adatbázis mentés notification megjelenítése 3 másodpercig"""
        if hasattr(self, 'db_notification_label'):
            self.db_notification_label.config(text="✓ Adatbázis mentve", foreground="green")
            # 3 másodperc után eltüntetés
            self.root.after(3000, self.hide_db_notification)
    
    def show_db_update_notification_debounced(self):
        """Adatbázis update notification megjelenítése debounce-olással (1 másodperc késleltetéssel)"""
        # Töröljük az előző timert, ha van
        if hasattr(self, 'db_update_notification_timer') and self.db_update_notification_timer:
            self.root.after_cancel(self.db_update_notification_timer)
        
        # Új timer beállítása
        def show_notification():
            if hasattr(self, 'db_notification_label'):
                self.db_notification_label.config(text="✓ Adatbázis frissítve", foreground="green")
                # 3 másodperc után eltüntetés
                self.root.after(3000, self.hide_db_notification)
            self.db_update_notification_timer = None
        
        self.db_update_notification_timer = self.root.after(1000, show_notification)  # 1 másodperc debounce
    
    def hide_db_notification(self):
        """Adatbázis notification elrejtése"""
        if hasattr(self, 'db_notification_label'):
            self.db_notification_label.config(text="")
    
    def update_estimated_end_time_from_progress(self, item_id, progress_msg):
        """Becsült befejezési idő számítása és frissítése a progress alapján (frame szám alapján számolódik)"""
        if not progress_msg or " / " not in progress_msg:
            return
        
        try:
            progress_parts = progress_msg.split(" / ")
            if len(progress_parts) != 2:
                return
            
            elapsed_str = progress_parts[0].strip()
            total_str = progress_parts[1].strip()
            elapsed_parts = elapsed_str.split(":")
            total_parts = total_str.split(":")
            
            if len(elapsed_parts) != 3 or len(total_parts) != 3:
                return
            
            elapsed_seconds = int(elapsed_parts[0]) * 3600 + int(elapsed_parts[1]) * 60 + int(elapsed_parts[2])
            total_seconds = int(total_parts[0]) * 3600 + int(total_parts[1]) * 60 + int(total_parts[2])
            
            if elapsed_seconds <= 0 or total_seconds <= 0 or item_id not in self.encoding_start_times:
                return
            
            start_time = self.encoding_start_times[item_id]
            current_time = time.time()
            elapsed_time = current_time - start_time
            
            if elapsed_time <= 0:
                return
            
            remaining_video_seconds = total_seconds - elapsed_seconds
            if remaining_video_seconds <= 0:
                return
            
            encoding_speed = elapsed_seconds / elapsed_time
            if encoding_speed <= 0:
                return
            
            remaining_encoding_time = remaining_video_seconds / encoding_speed
            estimated_end_time = current_time + remaining_encoding_time
            estimated_end_datetime = datetime.fromtimestamp(estimated_end_time)
            estimated_end_str = estimated_end_datetime.strftime("%Y-%m-%d %H:%M:%S")
            self.estimated_end_dates[item_id] = estimated_end_str
            
            # Frissítjük a completed_date-t a queue-n keresztül
            current_values = self.get_tree_values(item_id)
            self.encoding_queue.put(("update", item_id, 
                                    current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else "",
                                    current_values[self.COLUMN_INDEX['cq']] if len(current_values) > self.COLUMN_INDEX['cq'] else "-",
                                    current_values[self.COLUMN_INDEX['vmaf']] if len(current_values) > self.COLUMN_INDEX['vmaf'] else "-",
                                    current_values[self.COLUMN_INDEX['psnr']] if len(current_values) > self.COLUMN_INDEX['psnr'] else "-",
                                    progress_msg,
                                    current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-",
                                    current_values[self.COLUMN_INDEX['new_size']] if len(current_values) > self.COLUMN_INDEX['new_size'] else "-",
                                    current_values[self.COLUMN_INDEX['size_change']] if len(current_values) > self.COLUMN_INDEX['size_change'] else "-",
                                    estimated_end_str))
        except (tk.TclError, KeyError, AttributeError, IndexError, queue.Full):
            pass
    
    def clear_encoding_times(self, item_id):
        """Clear start time and estimated end time for a video.
        
        Args:
            item_id: Treeview item ID.
        """
        if item_id in self.encoding_start_times:
            del self.encoding_start_times[item_id]
        if item_id in self.estimated_end_dates:
            del self.estimated_end_dates[item_id]
    
    def get_tree_values(self, item_id, min_length=9):
        """Tree értékek lekérése és kiterjesztése szükség esetén"""
        current_values = list(self.tree.item(item_id)['values'])
        if len(current_values) < min_length:
            current_values.extend([''] * (min_length - len(current_values)))
        return current_values

    def _get_video_path_by_item(self, item_id):
        for video_path, vid_item_id in self.video_items.items():
            if vid_item_id == item_id:
                return video_path
        return None

    def _show_hidden_item_if_needed(self, item_id):
        """Reattaches a previously hidden item when its status is no longer completed."""
        if item_id not in getattr(self, 'hidden_items', set()):
            return
        if not hasattr(self, 'tree'):
            return
        video_path = self._get_video_path_by_item(item_id)
        if not video_path:
            return
        try:
            id_to_video_path = {vid: path for path, vid in self.video_items.items()}
            children = list(self.tree.get_children(""))
            target_order = self.video_order.get(video_path, float('inf'))
            insert_index = len(children)
            for idx, child_id in enumerate(children):
                child_video_path = id_to_video_path.get(child_id)
                if not child_video_path:
                    continue
                child_order = self.video_order.get(child_video_path, float('inf'))
                if target_order < child_order:
                    insert_index = idx
                    break
            insert_pos = tk.END if insert_index >= len(children) else insert_index
            reattach = getattr(self.tree, "reattach", None)
            if callable(reattach):
                reattach(item_id, "", insert_pos)
            else:
                self.tree.move(item_id, "", insert_pos)
            self.hidden_items.discard(item_id)
        except (tk.TclError, KeyError, AttributeError, ValueError):
            pass

    
    def has_pending_tasks(self):
        """Check if there are any pending tasks in the queue.
        
        Returns:
            bool: True if there are pending videos, False otherwise.
        """

        def log_result(result, reason):
            if LOAD_DEBUG:
                load_debug_log(f"has_pending_tasks -> {result} ({reason}) | items={len(self.video_items)} | videos={len(self.video_files)} | loading={self.is_loading_videos}")

        if not VMAF_QUEUE.empty():
            log_result(True, f"VMAF_QUEUE size={VMAF_QUEUE.qsize()}")
            return True
        if not AUDIO_EDIT_QUEUE.empty():
            log_result(True, f"AUDIO_EDIT_QUEUE size={AUDIO_EDIT_QUEUE.qsize()}")
            return True
        if not SVT_QUEUE.empty():
            log_result(True, f"SVT_QUEUE size={SVT_QUEUE.qsize()}")
            return True
        if getattr(self, 'manual_nvenc_tasks', None):
            log_result(True, "manual_nvenc_tasks pending")
            return True

        finished_codes = {
            'completed', 'completed_nvenc', 'completed_svt',
            'completed_copy', 'completed_exists',
            'failed', 'source_missing', 'file_missing', 'load_error'
        }

        # Végigmegyünk a videókon, de ha találunk egy pending videót, azonnal visszatérünk
        for video_path, item_id in self.video_items.items():
            try:
                current_values = self.tree.item(item_id, 'values')
                status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                status_code = normalize_status_to_code(status)

                # Tag-ek ellenőrzése - ha pending/encoding tag van, akkor biztosan van feladat
                # Ezt először ellenőrizzük, mert ez a leggyorsabb
                tags = self.tree.item(item_id, 'tags') or ()
                if any(tag in ('pending', 'encoding_nvenc', 'encoding_svt', 'needs_check', 'needs_check_nvenc', 'needs_check_svt', 'audio_edit') for tag in tags):
                    # Ha a tag pending/encoding, akkor biztosan van feladat, függetlenül a status_code-tól
                    if status_code not in finished_codes:
                        log_result(True, f"pending tag {tags} and status {status_code} for {video_path}")
                        return True
                    # Ha a status_code None, de pending tag van, akkor is van feladat
                    if status_code is None:
                        log_result(True, f"unknown status but pending tag {tags} for {video_path}")
                        return True

                # Ha nem tudtuk beazonosítani a kódot, de a tag 'pending'/'encoding', tekintsük feladatnak
                if status_code is None:
                    continue

                if status_code not in finished_codes:
                    log_result(True, f"status {status_code} for {video_path}")
                    return True
            except (tk.TclError, KeyError, AttributeError, IndexError) as e:
                # Ha hiba van egy videó ellenőrzésekor, folytatjuk a következővel
                if LOAD_DEBUG:
                    load_debug_log(f"has_pending_tasks: hiba videó ellenőrzésekor ({video_path}): {e}")
                continue

        log_result(False, "no pending items")
        return False

    def _reset_encoding_ui_if_idle(self, status_text=None):
        """Visszaállítja a vezérlőket, ha nincs aktív worker és nincsenek függő feladatok."""
        if (getattr(self, 'encoding_worker_running', False) or
                getattr(self, 'manual_nvenc_active', False) or
                getattr(self, 'vmaf_worker_active', False) or
                self.audio_edit_only_mode):
            return
        if not hasattr(self, 'tree'):
            return
        if self.has_pending_tasks():
            return
        self.is_encoding = False
        if hasattr(self, 'load_videos_btn'):
            self.load_videos_btn.config(state=tk.NORMAL)
        if hasattr(self, 'immediate_stop_button'):
            self.immediate_stop_button.config(state=tk.DISABLED)
        if hasattr(self, 'status_label'):
            self.status_label.config(text=status_text or t('status_ready'))
        self.update_start_button_state()

    def _get_validated_subtitles_for_video(self, video_path):
        """Get validated subtitles for a specific video.
        
        Args:
            video_path: Path to the video file.
            
        Returns:
            list: List of validated subtitle files.
        """
        subtitle_files = find_subtitle_files(video_path)
        valid, invalid = split_valid_invalid_subtitles(subtitle_files)
        if invalid:
            self._log_invalid_subtitles(video_path, invalid)
        # Visszaadja a valid subtitle fájlokat és az invalid-okat
        return valid, invalid

    def _log_invalid_subtitles(self, video_path, invalid_entries):
        if not invalid_entries:
            return
        if not hasattr(self, 'logged_invalid_subtitles'):
            self.logged_invalid_subtitles = set()
        for sub_path, _, reason in invalid_entries:
            cache_key = (str(video_path), str(sub_path))
            if cache_key in self.logged_invalid_subtitles:
                continue
            self.logged_invalid_subtitles.add(cache_key)
            reason_text = reason or "ismeretlen ok"
            message = f"⚠ Hibás felirat kihagyva a beágyazásból: {sub_path.name} ({reason_text}). Külső fájlként kerül átmásolásra."
            try:
                self.log_status(message)
            except Exception:
                pass
            if LOAD_DEBUG:
                load_debug_log(message)
            if LOG_WRITER:
                try:
                    LOG_WRITER.write(message + "\n")
                    LOG_WRITER.flush()
                except Exception:
                    pass

    def _copy_invalid_subtitles(self, invalid_subtitles, output_file):
        if not invalid_subtitles or not output_file:
            return
        for sub_path, lang_part, reason in invalid_subtitles:
            try:
                if not sub_path.exists():
                    msg = f"⚠ Hibás felirat nem található, kihagyva: {sub_path}"
                    if LOAD_DEBUG:
                        load_debug_log(msg)
                    if LOG_WRITER:
                        try:
                            LOG_WRITER.write(msg + "\n")
                            LOG_WRITER.flush()
                        except Exception:
                            pass
                    continue
                dest_name = output_file.stem
                if lang_part:
                    dest_name += f".{lang_part}"
                dest_name += sub_path.suffix
                dest_sub_path = output_file.parent / dest_name
                dest_sub_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(sub_path, dest_sub_path)
                copy_msg = f"⚠ Hibás felirat külső fájlként átmásolva: {dest_sub_path.name} ({reason})"
                try:
                    self.log_status(copy_msg)
                except Exception:
                    pass
                if LOAD_DEBUG:
                    load_debug_log(copy_msg)
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(copy_msg + "\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
            except (OSError, PermissionError) as copy_error:
                err_msg = f"⚠ Hibás felirat másolási hiba: {sub_path} -> {copy_error}"
                if LOAD_DEBUG:
                    load_debug_log(err_msg)
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(err_msg + "\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
    def _on_manual_nvenc_worker_finished(self):
        """Manuális NVENC worker leállása után frissíti az UI-t."""
        self.manual_nvenc_active = False
        self._reset_encoding_ui_if_idle()

    def _on_vmaf_worker_finished(self):
        """VMAF/PSNR worker leállása után frissíti az UI-t."""
        self.vmaf_worker_active = False
        self._reset_encoding_ui_if_idle()
    
    def update_start_button_state(self):
        """Update the state of the Start button based on pending tasks.
        
        Enables the button if there are pending tasks and encoding is not running.
        Disables it otherwise.
        """

        if LOAD_DEBUG:
            import traceback
            caller = traceback.extract_stack()[-2].name if len(traceback.extract_stack()) > 1 else "unknown"
            load_debug_log(f"update_start_button_state hívva: caller={caller} | is_encoding={self.is_encoding} | is_loading={getattr(self, 'is_loading_videos', False)} | graceful_stop={getattr(self, 'graceful_stop_requested', False)}")
        
        # Ha graceful stop kérvényezve van, a gomb inaktív legyen, amíg a leállítás folyamatban van
        if getattr(self, 'graceful_stop_requested', False):
            self.start_button.config(text=t('btn_stop'), command=self.stop_encoding_graceful, state=tk.DISABLED)
            self.immediate_stop_button.config(state=tk.NORMAL)  # Azonnali leállítás továbbra is aktív
            return
        
        if self.is_encoding:
            # Folyamatban van valami – Start gomb "Leállítás"-ként aktív
            self.start_button.config(text=t('btn_stop'), command=self.stop_encoding_graceful, state=tk.NORMAL)
            self.immediate_stop_button.config(state=tk.NORMAL)
            return
        if getattr(self, 'is_loading_videos', False):
            # Betöltés közben ne lehessen indítani
            self.start_button.config(text=t('btn_start'), command=self.start_encoding, state=tk.DISABLED)
            if hasattr(self, 'immediate_stop_button'):
                self.immediate_stop_button.config(state=tk.DISABLED)
            return
        has_tasks = self.has_pending_tasks()
        if has_tasks:
            if LOAD_DEBUG:
                load_debug_log(f"update_start_button_state: has_tasks=True, aktiváljuk a gombot")
            self.start_button.config(text=t('btn_start'), command=self.start_encoding, state=tk.NORMAL)
            self.immediate_stop_button.config(state=tk.DISABLED)
            if LOAD_DEBUG:
                load_debug_log(f"update_start_button_state: gomb aktiválva (state={self.start_button.cget('state')})")
            return

        if self.video_items and not self.is_loading_videos:
            # Ha vannak betöltött videók, engedjük a Start gombot akkor is, ha nem találtunk pending státuszt
            if LOAD_DEBUG:
                load_debug_log(f"update_start_button_state: video_items={len(self.video_items)}, aktiváljuk a gombot")
            self.start_button.config(text=t('btn_start'), command=self.start_encoding, state=tk.NORMAL)
        else:
            if LOAD_DEBUG:
                load_debug_log(f"update_start_button_state: nincs video_items vagy loading, inaktív gomb")
            self.start_button.config(text=t('btn_start'), command=self.start_encoding, state=tk.DISABLED)
        self.immediate_stop_button.config(state=tk.DISABLED)

    def _update_nvenc_checkbox_text(self):
        if not hasattr(self, 'nvenc_checkbutton'):
            return
        nvenc_text = t('nvenc_enabled')
        if getattr(self, 'detected_gpu_name', None):
            nvenc_text += f" ({self.detected_gpu_name})"
        self.nvenc_checkbutton.config(text=nvenc_text)

    def _get_context_label(self, mode, completed):
        key_map = {
            'auto': 'context_auto',
            'svt': 'context_svt',
            'nvenc': 'context_nvenc'
        }
        base = key_map.get(mode, 'context_auto')
        suffix = '_reencode' if completed else '_encode'
        return t(base + suffix)

    def log_status(self, message):
        if LOG_WRITER:
            try:
                LOG_WRITER.write(message + "\n")
                LOG_WRITER.flush()
            except Exception:
                pass

    def normalize_queue_statuses(self):
        """NVENC engedély változáskor frissíti a várólisták státuszait."""
        nvenc_on = self.nvenc_enabled.get()
        changed = False
        for video_path, item_id in self.video_items.items():
            values = list(self.tree.item(item_id, 'values'))
            if len(values) < 2:
                continue
            status_text = values[self.COLUMN_INDEX['status']]
            status_code = normalize_status_to_code(status_text)
            new_status_code = status_code
            if not nvenc_on and status_code == 'nvenc_queue':
                new_status_code = 'svt_queue'
            elif nvenc_on and status_code == 'svt_queue':
                # ne írjuk át automatikusan, csak akkor ha eredetileg NVENC sorból jött? kihagy
                new_status_code = 'svt_queue'
            if new_status_code != status_code:
                values[self.COLUMN_INDEX['status']] = status_code_to_localized(new_status_code)
                self.tree.item(item_id, values=tuple(values))
                changed = True
        if changed:
            # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
            pass

    def _on_nvenc_toggle(self, *args):
        self._update_nvenc_checkbox_text()
        if not self.nvenc_enabled.get():
            self.normalize_queue_statuses()
        self.update_start_button_state()

    def schedule_auto_encode(self, video_path, item_id, encoder='auto', prompt=True):
        """Átkódolás NVENC-vel (vagy automatikus döntéssel)."""
        use_nvenc = self.nvenc_enabled.get()
        if encoder == 'nvenc':
            if not self.nvenc_enabled.get():
                if prompt:
                    messagebox.showwarning("Figyelem", "NVENC le van tiltva, SVT-AV1 lesz használva.")
                return self.reencode_with_svt_av1(video_path, item_id, prompt=prompt)
            use_nvenc = True
        elif encoder == 'svt':
            return self.reencode_with_svt_av1(video_path, item_id, prompt=prompt)
        elif encoder == 'auto':
            if not self.nvenc_enabled.get():
                return self.reencode_with_svt_av1(video_path, item_id, prompt=prompt)

        output_file = self.video_to_output.get(video_path)
        if not output_file:
            output_file = get_output_filename(video_path, self.source_path, self.dest_path)
            self.video_to_output[video_path] = output_file

        if output_file and output_file.exists():
            try:
                output_file.unlink()
            except Exception as e:
                if prompt:
                    messagebox.showerror("Hiba", f"{t('msg_delete_failed')}\n{e}")
                return False

        orig_values = self.tree.item(item_id, 'values')
        orig_size_str = orig_values[self.COLUMN_INDEX['orig_size']] if len(orig_values) > self.COLUMN_INDEX['orig_size'] else "-"
        status_text = t('status_nvenc_queue')
        completed_date = ""

        new_values = list(orig_values)
        if len(new_values) < len(self.COLUMN_INDEX):
            new_values.extend([''] * (len(self.COLUMN_INDEX) - len(new_values)))
        new_values[self.COLUMN_INDEX['status']] = status_text
        new_values[self.COLUMN_INDEX['cq']] = "-"
        new_values[self.COLUMN_INDEX['vmaf']] = "-"
        new_values[self.COLUMN_INDEX['psnr']] = "-"
        new_values[self.COLUMN_INDEX['progress']] = "-"
        new_values[self.COLUMN_INDEX['new_size']] = "-"
        new_values[self.COLUMN_INDEX['size_change']] = "-"
        # Megtartjuk a duration és frames értékeket
        new_values[self.COLUMN_INDEX['completed_date']] = completed_date
        self.tree.item(item_id, values=tuple(new_values))

        self.encoding_queue.put(("update", item_id, status_text, "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
        self.encoding_queue.put(("tag", item_id, "pending"))
        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
        if prompt:
            messagebox.showinfo("Indítva", f"{t('msg_reencode_added').format(encoder='NVENC')}\n{video_path.name}")
        else:
            self.log_status(f"✓ NVENC sorba állítva: {video_path.name}")
        self.update_start_button_state()
        return True

    def bulk_schedule_auto(self, item_ids, encoder='auto'):
        processed = 0
        for item_id in item_ids:
            video_path = self._get_video_path_by_item(item_id)
            if not video_path:
                continue
            if encoder == 'svt':
                if self.reencode_with_svt_av1(video_path, item_id, prompt=False):
                    processed += 1
            else:
                if self.schedule_auto_encode(video_path, item_id, encoder=encoder, prompt=False):
                    processed += 1
        if processed:
            self.log_status(f"✓ {processed} videó sorba állítva ({encoder}).")
            self.update_start_button_state()
    
    def calculate_file_sizes(self, video_path, output_file):
        """Fájlméretek számítása MB-ban és változás százalékban"""
        if not video_path.exists():
            return 0, 0, 0
        orig_size_mb = video_path.stat().st_size / (1024**2)
        new_size_mb = output_file.stat().st_size / (1024**2) if output_file.exists() else 0
        change_percent = ((new_size_mb - orig_size_mb) / orig_size_mb) * 100 if orig_size_mb > 0 else 0
        return orig_size_mb, new_size_mb, change_percent
    
    def mark_encoding_completed(self, item_id, status_text, cq_str, vmaf_str, psnr_str, orig_size_str, new_size_mb, change_percent, completed_date=None):
        """Kódolás befejezésének jelölése és státusz frissítése"""
        if completed_date is None:
            completed_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        status_code = normalize_status_to_code(status_text)
        self.clear_encoding_times(item_id)
        new_size_str = f"{format_localized_number(new_size_mb, decimals=1)} MB"
        change_percent_str = f"{format_localized_number(change_percent, decimals=2, show_sign=True)}%"
        self.encoding_queue.put(("update", item_id, status_text, cq_str, vmaf_str, psnr_str, "100%", orig_size_str, new_size_str, change_percent_str, completed_date))
        self.encoding_queue.put(("tag", item_id, "completed"))
        self.encoding_queue.put(("progress_bar", 0))
        self.encoding_queue.put(("update_summary",))
        
        # Adatbázis frissítése minden egyes fájl feldolgozása után
        # Megkeressük a video_path-et
        video_path = None
        for vp, vid in self.video_items.items():
            if vid == item_id:
                video_path = vp
                break
        
        if video_path:
            # Háttérszálban frissítjük az adatbázist, hogy ne blokkolja az encoding folyamatot
            def update_db_in_thread():
                try:
                    self.update_single_video_in_db(video_path, item_id, status_text, cq_str, vmaf_str, psnr_str, orig_size_str, new_size_mb, change_percent, completed_date)
                except Exception as e:
                    # Csendes hiba - ne zavarjuk meg az encoding folyamatot
                    if LOG_WRITER:
                        try:
                            LOG_WRITER.write(f"⚠ [mark_encoding_completed] Adatbázis frissítés hiba: {e} | video: {video_path}\n")
                            LOG_WRITER.flush()
                        except Exception:
                            pass
            
            db_thread = threading.Thread(target=update_db_in_thread, daemon=True)
            db_thread.start()
        
        # Ha az automatikus VMAF/PSNR számítás be van kapcsolva, ütemezzük
        if self.auto_vmaf_psnr.get():
            # A video_items dictionary-ben van tárolva: video_path -> item_id
            # Megkeressük a fordított irányt
            if not video_path:
                video_path = None
                for vp, vid in self.video_items.items():
                    if vid == item_id:
                        video_path = vp
                        break
            
            if video_path:
                output_file = self.video_to_output.get(video_path)
                if not output_file:
                    output_file = get_output_filename(video_path, self.source_path, self.dest_path)
                
                if output_file and output_file.exists():
                    # VMAF/PSNR számításra ütemezzük
                    vmaf_task = {
                        'video_path': video_path,
                        'output_file': output_file,
                        'item_id': item_id,
                        'orig_size_str': orig_size_str
                    }
                    if status_code:
                        vmaf_task['final_status_code'] = status_code
                    VMAF_QUEUE.put(vmaf_task)
                    new_size_str = f"{format_localized_number(new_size_mb, decimals=1)} MB"
                    change_percent_str = f"{format_localized_number(change_percent, decimals=2, show_sign=True)}%"
                    self.encoding_queue.put(("update", item_id, t('status_vmaf_psnr_waiting'), cq_str, vmaf_str, "-", "-", orig_size_str, new_size_str, change_percent_str, completed_date))
                    self.encoding_queue.put(("tag", item_id, "vmaf_waiting"))
                    if hasattr(self, 'root'):
                        self.root.after(0, self.ensure_vmaf_worker_running)

    def start_estimated_end_timer(self):
        """Timer indítása a becsült befejezési idő frissítéséhez (10 másodpercenként)"""
        def update_estimated_end_times():
            if not hasattr(self, 'encoding_start_times'):
                self.encoding_start_times = {}
            
            current_time = time.time()
            items_to_remove = []
            
            for item_id, start_time in list(self.encoding_start_times.items()):
                try:
                    current_values = self.tree.item(item_id, 'values')
                    if len(current_values) < len(self.COLUMN_INDEX):
                        continue
                    
                    status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                    progress = current_values[self.COLUMN_INDEX['progress']] if len(current_values) > self.COLUMN_INDEX['progress'] else ""
                    completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                    
                    # Ha kész vagy sikertelen, töröljük a kezdési időt
                    if is_status_completed(status) or is_status_failed(status):
                        items_to_remove.append(item_id)
                        continue
                    
                    # Ha a progress "100%" vagy hasonló, akkor a kódolás befejeződött vagy éppen befejeződik
                    # Ne írjuk felül a completed_date-t, mert az már a valódi befejezési dátum lehet
                    progress_stripped = progress.strip() if progress else ""
                    is_percentage_complete = False
                    if progress_stripped:
                        if progress_stripped == "100%":
                            is_percentage_complete = True
                        elif progress_stripped.endswith("%"):
                            # Ellenőrizzük, hogy százalékos érték-e (pl. "99.5%")
                            try:
                                percent_value = float(progress_stripped.replace("%", ""))
                                if percent_value >= 99.0:  # 99% felett tekintsük befejezettnek
                                    is_percentage_complete = True
                            except ValueError:
                                pass
                    
                    if is_percentage_complete:
                        # Ha már van valódi befejezési dátum (nem "-" és nem üres), ne írjuk felül
                        if completed_date and completed_date != "-" and completed_date.strip():
                            # Ellenőrizzük, hogy dátum formátumú-e (YYYY-MM-DD HH:MM:SS)
                            try:
                                datetime.strptime(completed_date, "%Y-%m-%d %H:%M:%S")
                                # Ha dátum formátumú, akkor valószínűleg már befejeződött, ne írjuk felül
                                continue
                            except ValueError:
                                # Ha nem dátum formátumú, akkor lehet, hogy még becsült befejezési idő
                                pass
                    
                    # VMAF/PSNR számítás során is frissítsük a becsült befejezési időt
                    # (a status_vmaf_calculating státusz esetén is)
                    is_vmaf_calculating = (status == t('status_vmaf_calculating'))
                    
                    # Ha VMAF/PSNR számítás folyamatban van, de nincs még progress információ,
                    # akkor az első 10 másodpercben "-" jelenjen meg, majd 10 másodperc után kezdjen el számolni
                    # VMAF/PSNR számítás során MINDIG frissítsük a becsült befejezési időt (az átkódolás dátuma helyett)
                    if is_vmaf_calculating and (not progress or progress == "-" or " / " not in progress):
                        # Eltelt idő a kezdés óta
                        elapsed_time = current_time - start_time
                        
                        # Az első 10 másodpercben "-" jelenjen meg
                        if elapsed_time < 10:
                            # "-" beállítása (ne frissítsük, ha már "-" van)
                            if completed_date != "-":
                                self.encoding_queue.put(("update", item_id, status,
                                                        current_values[self.COLUMN_INDEX['cq']] if len(current_values) > self.COLUMN_INDEX['cq'] else "-",
                                                        current_values[self.COLUMN_INDEX['vmaf']] if len(current_values) > self.COLUMN_INDEX['vmaf'] else "-",
                                                        current_values[self.COLUMN_INDEX['psnr']] if len(current_values) > self.COLUMN_INDEX['psnr'] else "-",
                                                        progress if progress else "-",
                                                        current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-",
                                                        current_values[self.COLUMN_INDEX['new_size']] if len(current_values) > self.COLUMN_INDEX['new_size'] else "-",
                                                        current_values[self.COLUMN_INDEX['size_change']] if len(current_values) > self.COLUMN_INDEX['size_change'] else "-",
                                                        "-"))
                        else:
                            # 10 másodperc után számoljunk egy kezdeti becsült befejezési időt
                            # Kezdeti becslés: feltételezzük, hogy a VMAF számítás még legalább 2x annyi ideig tart
                            # (ez egy konzervatív becslés, amíg nem jön progress információ)
                            # 10 másodpercenként aktualizáljuk a becsült VMAF újraellenőrzés befejezési dátumidő pontot
                            estimated_total_time = elapsed_time * 3  # 3x az eltelt idő
                            estimated_end_time = start_time + estimated_total_time
                            
                            # Becsült befejezési idő formázása
                            estimated_end_datetime = datetime.fromtimestamp(estimated_end_time)
                            estimated_end_str = estimated_end_datetime.strftime("%Y-%m-%d %H:%M:%S")
                            
                            # Tároljuk a becsült befejezési időt
                            self.estimated_end_dates[item_id] = estimated_end_str
                            
                            # VMAF számítás során mindig frissítsük a becsült befejezési időt
                            # (még akkor is, ha a completed_date dátum formátumú - az átkódolás dátuma)
                            # 10 másodpercenként aktualizáljuk a becsült VMAF újraellenőrzés befejezési dátumidő pontot
                            self.encoding_queue.put(("update", item_id, status,
                                                    current_values[self.COLUMN_INDEX['cq']] if len(current_values) > self.COLUMN_INDEX['cq'] else "-",
                                                    current_values[self.COLUMN_INDEX['vmaf']] if len(current_values) > self.COLUMN_INDEX['vmaf'] else "-",
                                                    current_values[self.COLUMN_INDEX['psnr']] if len(current_values) > self.COLUMN_INDEX['psnr'] else "-",
                                                    progress if progress else "-",
                                                    current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-",
                                                    current_values[self.COLUMN_INDEX['new_size']] if len(current_values) > self.COLUMN_INDEX['new_size'] else "-",
                                                    current_values[self.COLUMN_INDEX['size_change']] if len(current_values) > self.COLUMN_INDEX['size_change'] else "-",
                                                    estimated_end_str))
                    
                    # Ha van progress információ (pl. "00:05:23 / 01:45:30")
                    # MINDIG számoljunk, ha van progress információ, még akkor is, ha nem változott
                    if progress and " / " in progress:
                        try:
                            # Progress formátum: "HH:MM:SS / HH:MM:SS"
                            progress_parts = progress.split(" / ")
                            if len(progress_parts) == 2:
                                elapsed_str = progress_parts[0].strip()
                                total_str = progress_parts[1].strip()
                                
                                # Időtartamok másodpercre konvertálása
                                elapsed_parts = elapsed_str.split(":")
                                total_parts = total_str.split(":")
                                
                                if len(elapsed_parts) == 3 and len(total_parts) == 3:
                                    elapsed_seconds = int(elapsed_parts[0]) * 3600 + int(elapsed_parts[1]) * 60 + int(elapsed_parts[2])
                                    total_seconds = int(total_parts[0]) * 3600 + int(total_parts[1]) * 60 + int(total_parts[2])
                                    
                                    if elapsed_seconds <= 0 or total_seconds <= 0:
                                        continue
                                    
                                    # Eltelt idő a kódolás kezdésétől
                                    elapsed_time = current_time - start_time
                                    
                                    if elapsed_time <= 0:
                                        continue
                                    
                                    # Hátralévő videó másodpercek
                                    remaining_video_seconds = total_seconds - elapsed_seconds
                                    
                                    if remaining_video_seconds <= 0:
                                        # Ha már kész, akkor nincs mit becsülni
                                        continue
                                    
                                    # Kódolás sebessége: videó másodpercek / kódolás másodpercek
                                    encoding_speed = elapsed_seconds / elapsed_time
                                    
                                    if encoding_speed > 0:
                                        # Hátralévő kódolás idő = hátralévő videó másodpercek / kódolás sebesség
                                        remaining_encoding_time = remaining_video_seconds / encoding_speed
                                        estimated_end_time = current_time + remaining_encoding_time
                                        
                                        # Becsült befejezési idő formázása
                                        estimated_end_datetime = datetime.fromtimestamp(estimated_end_time)
                                        estimated_end_str = estimated_end_datetime.strftime("%Y-%m-%d %H:%M:%S")
                                        
                                        # MINDIG frissítsük a becsült befejezési időt
                                        self.estimated_end_dates[item_id] = estimated_end_str
                                        self.encoding_queue.put(("update", item_id, status,
                                                                current_values[self.COLUMN_INDEX['cq']] if len(current_values) > self.COLUMN_INDEX['cq'] else "-",
                                                                current_values[self.COLUMN_INDEX['vmaf']] if len(current_values) > self.COLUMN_INDEX['vmaf'] else "-",
                                                                current_values[self.COLUMN_INDEX['psnr']] if len(current_values) > self.COLUMN_INDEX['psnr'] else "-",
                                                                progress,
                                                                current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-",
                                                                current_values[self.COLUMN_INDEX['new_size']] if len(current_values) > self.COLUMN_INDEX['new_size'] else "-",
                                                                current_values[self.COLUMN_INDEX['size_change']] if len(current_values) > self.COLUMN_INDEX['size_change'] else "-",
                                                                estimated_end_str))
                                    else:
                                        # Ha encoding_speed 0 vagy negatív, akkor progress_ratio módszert használjuk
                                        progress_ratio = elapsed_seconds / total_seconds
                                        if progress_ratio > 0:
                                            estimated_total_time = elapsed_time / progress_ratio
                                            estimated_end_time = start_time + estimated_total_time
                                            
                                            # Becsült befejezési idő formázása
                                            estimated_end_datetime = datetime.fromtimestamp(estimated_end_time)
                                            estimated_end_str = estimated_end_datetime.strftime("%Y-%m-%d %H:%M:%S")
                                            
                                            # MINDIG frissítsük a becsült befejezési időt
                                            self.estimated_end_dates[item_id] = estimated_end_str
                                            self.encoding_queue.put(("update", item_id, status,
                                                                    current_values[self.COLUMN_INDEX['cq']] if len(current_values) > self.COLUMN_INDEX['cq'] else "-",
                                                                    current_values[self.COLUMN_INDEX['vmaf']] if len(current_values) > self.COLUMN_INDEX['vmaf'] else "-",
                                                                    current_values[self.COLUMN_INDEX['psnr']] if len(current_values) > self.COLUMN_INDEX['psnr'] else "-",
                                                                    progress,
                                                                    current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-",
                                                                    current_values[self.COLUMN_INDEX['new_size']] if len(current_values) > self.COLUMN_INDEX['new_size'] else "-",
                                                                    current_values[self.COLUMN_INDEX['size_change']] if len(current_values) > self.COLUMN_INDEX['size_change'] else "-",
                                                                    estimated_end_str))
                                        else:
                                            continue
                        except (tk.TclError, KeyError, AttributeError, IndexError, queue.Full, ValueError, TypeError) as e:
                            # Hiba esetén naplózzuk, hogy lássuk mi a probléma
                            import traceback
                            print(f"TIMER HIBA: {e}")
                            traceback.print_exc()
                except (tk.TclError, KeyError, AttributeError, IndexError):
                    # Ha az item_id már nem létezik, kihagyjuk
                    continue
            
            # Törlés a befejezett/sikertelen tételekből
            for item_id in items_to_remove:
                self.clear_encoding_times(item_id)
            
            # Timer újraindítása 10 másodperc múlva
            if hasattr(self, 'root') and self.root.winfo_exists():
                self.estimated_end_timer = self.root.after(10000, update_estimated_end_times)
        
        # Első indítás 10 másodperc múlva
        if hasattr(self, 'root') and self.root.winfo_exists():
            self.estimated_end_timer = self.root.after(10000, update_estimated_end_times)
    
    def toggle_hide_completed(self):
        """Elkészültek elrejtése/megjelenítése"""
        hide = self.hide_completed.get()
        
        # Item ID -> videó útvonal gyors kereséshez
        id_to_video_path = {item_id: video_path for video_path, item_id in self.video_items.items()}

        # Összes videó végigjárása
        for video_path, item_id in list(self.video_items.items()):
            # Csak a létező item_id-kat kezeljük
            try:
                current_values = self.tree.item(item_id, 'values')
                status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                
                # Ha kész státuszú
                if is_status_completed(status):
                    if hide:
                        # Elrejtés - csak ha még nem rejtett
                        try:
                            # Ellenőrizzük, hogy látható-e
                            parent = self.tree.parent(item_id)
                            if parent != "" or item_id in self.tree.get_children():
                                self.tree.detach(item_id)
                                self.hidden_items.add(item_id)
                        except (tk.TclError, KeyError, AttributeError):
                            pass
                    else:
                        # Megjelenítés
                        if item_id in self.hidden_items:
                            try:
                                children = list(self.tree.get_children(""))
                                target_order = self.video_order.get(video_path, float('inf'))
                                insert_index = len(children)

                                for idx, child_id in enumerate(children):
                                    child_video_path = id_to_video_path.get(child_id)
                                    if not child_video_path:
                                        continue
                                    child_order = self.video_order.get(child_video_path, float('inf'))
                                    if target_order < child_order:
                                        insert_index = idx
                                        break

                                insert_pos = tk.END if insert_index >= len(children) else insert_index

                                reattach = getattr(self.tree, "reattach", None)
                                if callable(reattach):
                                    reattach(item_id, "", insert_pos)
                                else:
                                    self.tree.move(item_id, "", insert_pos)
                            except (tk.TclError, KeyError, AttributeError, ValueError):
                                # Ha sikertelen, maradjon rejtve, hogy később újra próbálhassuk
                                continue
                            else:
                                self.hidden_items.discard(item_id)
            except (tk.TclError, KeyError, AttributeError):
                # Ha az item_id már nem létezik, kihagyjuk
                continue
        
        # JAVÍTÁS: Frissítjük a tree widget-et a megjelenítés után
        # Ez biztosítja, hogy minden elem látható legyen, beleértve az elsőt is
        self.tree.update_idletasks()
        
        # Extra biztonsági réteg: ellenőrizzük, hogy tényleg minden látható-e
        if not hide:
            # Ha megjelenítés történt, győződjünk meg róla, hogy a tree látja az elemeket
            self.tree.event_generate("<<TreeviewSelect>>")
            self.tree.update()
    
    def change_language(self, event=None):
        """Nyelv váltása"""
        global CURRENT_LANGUAGE
        selected_display = self.language_var.get()
        # Megkeressük a nyelv kódot
        if selected_display == t('hungarian'):
            new_lang = 'hu'
        elif selected_display == t('english'):
            new_lang = 'en'
        else:
            return
        
        if new_lang != CURRENT_LANGUAGE:
            CURRENT_LANGUAGE = new_lang
            # GUI frissítése
            self.root.title(t('app_title'))
            
            # Felső címkék és gombok frissítése
            self.language_label.config(text=t('language'), width=18, anchor=tk.W)
            self.ffmpeg_label.config(text=t('ffmpeg_path'), width=18, anchor=tk.W)
            self.vdub_label.config(text=t('virtualdub_path'), width=18, anchor=tk.W)
            self.abav1_label.config(text=t('abav1_path'), width=18, anchor=tk.W)
            self.ffmpeg_browse_btn.config(text=t('browse'))
            self.vdub_browse_btn.config(text=t('browse'))
            self.abav1_browse_btn.config(text=t('browse'))
            self.source_label.config(text=t('source'), width=12, anchor=tk.W)
            self.dest_label.config(text=t('dest'), width=12, anchor=tk.W)
            self.source_browse_btn.config(text=t('browse'))
            self.dest_browse_btn.config(text=t('browse'))
            
            # Mezők szélességének megtartása
            self.source_entry.config(width=50)
            self.dest_entry.config(width=50)
            self.debug_checkbutton.config(text=t('debug_mode'))
            if hasattr(self, 'auto_vmaf_psnr_checkbutton'):
                self.auto_vmaf_psnr_checkbutton.config(text=t('auto_vmaf_psnr'))
            self.load_videos_btn.config(text=t('load_videos'))
            
            # Jobb oldali címkék frissítése (fix szélességgel - minden címke 20 karakter széles, hogy a csúszkák ugyanarról a helyről kezdődjenek)
            self.min_vmaf_label.config(text=t('min_vmaf'), width=20, anchor=tk.W)
            self.vmaf_fallback_label.config(text=t('vmaf_fallback'), width=20, anchor=tk.W)
            self.max_encoded_label.config(text=t('max_encoded'), width=20, anchor=tk.W)
            self.resize_checkbox.config(text=t('resize_height'))
            self.skip_av1_checkbutton.config(text=t('skip_av1'))
            if hasattr(self, 'nvenc_workers_label'):
                self.nvenc_workers_label.config(text=t('nvenc_workers'), width=20, anchor=tk.W)
            
            # Hangdinamika kompresszió frissítése
            if hasattr(self, 'audio_compression_checkbutton'):
                self.audio_compression_checkbutton.config(text=t('audio_compression'))
            if hasattr(self, 'audio_compression_combo'):
                self.audio_compression_combo['values'] = [t('audio_compression_fast'), t('audio_compression_dialogue')]
                # Jelenlegi érték frissítése (a változó értéke 'fast' vagy 'dialogue')
                current_value = self.audio_compression_method.get()
                if current_value == 'fast':
                    self.audio_compression_combo.set(t('audio_compression_fast'))
                elif current_value == 'dialogue':
                    self.audio_compression_combo.set(t('audio_compression_dialogue'))
                else:
                    # Ha nem ismert érték, alapértelmezett 'fast'
                    self.audio_compression_method.set('fast')
                    self.audio_compression_combo.set(t('audio_compression_fast'))
            
            # NVENC checkbox frissítése
            nvenc_text = t('nvenc_enabled')
            if hasattr(self, 'detected_gpu_name') and self.detected_gpu_name:
                nvenc_text += f" ({self.detected_gpu_name})"
            if hasattr(self, 'nvenc_checkbutton'):
                self.nvenc_checkbutton.config(text=nvenc_text)
            
            # Notebook címkék frissítése
            if hasattr(self, 'videos_tab'):
                try:
                    self.notebook.tab(self.videos_tab, text=t('videos_tab'))
                except tk.TclError:
                    pass
            if hasattr(self, 'svt_tab'):
                try:
                    self.notebook.tab(self.svt_tab, text=t('svt_console'))
                except tk.TclError:
                    pass
            self._refresh_nvenc_console_tab_titles()
            
            # Táblázat fejlécek frissítése
            self.tree.heading("#0", text=t('column_order'))
            self.tree.heading("video_name", text=t('column_video'))
            self.tree.heading("status", text=t('column_status'))
            self.tree.heading("cq", text=t('column_cq'))
            self.tree.heading("vmaf", text=t('column_vmaf'))
            self.tree.heading("psnr", text=t('column_psnr'))
            self.tree.heading("progress", text=t('column_progress'))
            self.tree.heading("orig_size", text=t('column_orig_size'))
            self.tree.heading("new_size", text=t('column_new_size'))
            self.tree.heading("size_change", text=t('column_size_change'))
            self.tree.heading("completed_date", text=t('column_completed'))
            
            # Alsó gombok frissítése
            self.start_button.config(text=t('btn_start'))
            self.immediate_stop_button.config(text=t('btn_immediate_stop'))
            self.clear_table_btn.config(text=t('btn_clear_table'))
            self.hide_completed_checkbutton.config(text=t('btn_hide_completed'))
            
            # Státusz címke frissítése
            self.status_label.config(text=t('status_ready'))
            
            # Táblázat státuszüzeneteinek frissítése
            for video_path, item_id in self.video_items.items():
                current_values = list(self.tree.item(item_id, 'values'))
                if len(current_values) > self.COLUMN_INDEX['status']:
                    current_status = current_values[self.COLUMN_INDEX['status']]
                    # CRF keresés státuszok kezelése VMAF értékkel
                    import re
                    crf_match = re.search(r'(NVENC|SVT-AV1)\s+CRF\s+(?:keresés|search)\s*\(VMAF(?:\s+fallback)?:\s*([\d.]+)\)', current_status)
                    if crf_match:
                        encoder = crf_match.group(1)
                        vmaf_value = crf_match.group(2)
                        is_fallback = 'fallback' in crf_match.group(0)
                        if encoder == 'NVENC':
                            base_status = t('status_nvenc_crf_search')
                        else:
                            base_status = t('status_svt_crf_search')
                        # Eltávolítjuk a "..." végét, ha van
                        base_status = base_status.rstrip('...')
                        if is_fallback:
                            new_status = f"{base_status} (VMAF fallback: {vmaf_value})..."
                        else:
                            new_status = f"{base_status} (VMAF: {vmaf_value})..."
                        current_values[self.COLUMN_INDEX['status']] = new_status
                        self.tree.item(item_id, values=tuple(current_values))
                        continue
                    
                    # Státusz kód normalizálása és újrafordítása
                    status_code = normalize_status_to_code(current_status)
                    if status_code:
                        # Ha van státusz kód, fordítjuk le az új nyelvre
                        new_status = status_code_to_localized(status_code)
                        current_values[self.COLUMN_INDEX['status']] = new_status
                        self.tree.item(item_id, values=tuple(current_values))
                    elif current_status:
                        # Ha nincs státusz kód, próbáljuk meg fordítani a translate_status-szal
                        translated_status = translate_status(current_status)
                        if translated_status != current_status:
                            current_values[self.COLUMN_INDEX['status']] = translated_status
                            self.tree.item(item_id, values=tuple(current_values))
            
            # Nyelvválasztó frissítése
            lang_display = {'hu': t('hungarian'), 'en': t('english')}
            self.lang_combo['values'] = [lang_display['hu'], lang_display['en']]
            self.lang_combo.set(lang_display.get(CURRENT_LANGUAGE, CURRENT_LANGUAGE))

    def on_window_resize(self, event=None):
        """Reszponzív layout: mezők és csúszkák méretének beállítása az ablak szélessége alapján"""
        # Csak az ablak resize eseményére reagálunk
        if event is None or event.widget != self.root:
            return
        
        if not hasattr(self, 'source_entry') or not hasattr(self, 'vmaf_slider'):
            return  # Még nincs inicializálva a UI
        
        try:
            # Ablak szélessége
            window_width = self.root.winfo_width()
            
            # Ha még nincs inicializálva az ablak, várunk
            if window_width < 100:
                return
            
            # Alapértelmezett ablak szélesség (1400px)
            default_width = 1400
            
            # Számítási arány (minimum 0.5, maximum 1.5)
            ratio = max(0.5, min(1.5, window_width / default_width))
            
            # Entry mezők új szélessége
            new_entry_width_source = max(
                self.min_entry_width_source,
                int(self.default_entry_width_source * ratio)
            )
            new_entry_width_path = max(
                self.min_entry_width_path,
                int(self.default_entry_width_path * ratio)
            )
            
            # Csúszkák új hossza
            new_slider_length = max(
                self.min_slider_length,
                int(self.default_slider_length * ratio)
            )
            
            # Entry mezők frissítése
            if hasattr(self, 'source_entry'):
                self.source_entry.config(width=new_entry_width_source)
            if hasattr(self, 'dest_entry'):
                self.dest_entry.config(width=new_entry_width_source)
            if hasattr(self, 'ffmpeg_entry'):
                self.ffmpeg_entry.config(width=new_entry_width_path)
            if hasattr(self, 'vdub_entry'):
                self.vdub_entry.config(width=new_entry_width_path)
            if hasattr(self, 'abav1_entry'):
                self.abav1_entry.config(width=new_entry_width_path)
            
            # Csúszkák frissítése
            if hasattr(self, 'vmaf_slider'):
                self.vmaf_slider.config(length=new_slider_length)
            if hasattr(self, 'vmaf_step_slider'):
                self.vmaf_step_slider.config(length=new_slider_length)
            if hasattr(self, 'max_encoded_slider'):
                self.max_encoded_slider.config(length=new_slider_length)
            if hasattr(self, 'resize_slider'):
                self.resize_slider.config(length=new_slider_length)
        except Exception:
            # Hiba esetén ne akadjon el
            pass

    def update_vmaf_label(self, value):
        rounded_value = round(float(value) * 2) / 2
        self.min_vmaf.set(rounded_value)
        self.vmaf_value_label.config(text=format_localized_number(rounded_value, decimals=1))
    
    def update_vmaf_step_label(self, value):
        rounded_value = round(float(value) * 10) / 10
        self.vmaf_step.set(rounded_value)
        self.vmaf_step_value_label.config(text=format_localized_number(rounded_value, decimals=1))
    
    def update_max_encoded_label(self, value):
        int_value = int(float(value))
        self.max_encoded_percent.set(int_value)
        self.max_encoded_value_label.config(text=f"{int_value}%")
        self._save_settings_debounced()  # Automatikus mentés debounce-szal
    
    def toggle_resize_slider(self):
        """Megjeleníti vagy elrejti a resize csúszkát a checkbox állapota szerint."""
        if self.resize_enabled.get():
            self.resize_slider.pack(side=tk.LEFT, padx=5)
            self.resize_value_label.pack(side=tk.LEFT, padx=5)
        else:
            self.resize_slider.pack_forget()
            self.resize_value_label.pack_forget()
        self._save_settings_debounced()  # Automatikus mentés debounce-szal
    
    def update_resize_label(self, value):
        rounded_value = round(float(value) / 10) * 10
        self.resize_height.set(rounded_value)
        self.resize_value_label.config(text=f"{rounded_value}p")
        self._save_settings_debounced()  # Automatikus mentés debounce-szal

    def update_nvenc_workers_label(self, value):
        try:
            workers = int(round(float(value)))
        except (ValueError, TypeError):
            workers = int(self.nvenc_worker_count.get())
        max_workers = self.max_nvenc_consoles if hasattr(self, 'max_nvenc_consoles') else 3
        workers = max(1, min(max_workers, workers))
        self.nvenc_worker_count.set(workers)
        if hasattr(self, 'nvenc_workers_value_label'):
            self.nvenc_workers_value_label.config(text=str(workers))
        self.refresh_nvenc_console_tabs(workers)
        self._save_settings_debounced()  # Automatikus mentés debounce-szal
    
    def update_svt_preset_label(self, value):
        int_value = int(float(value))
        self.svt_preset.set(int_value)
        self.svt_preset_value_label.config(text=str(int_value))
        self._save_settings_debounced()  # Automatikus mentés debounce-szal
    
    def on_double_click(self, event):
        """Handle double-click event on Treeview items.
        
        Opens the encoded video file with the default system player if it exists.
        """
        # Ellenőrizzük, hogy a header területén történt-e a kattintás
        region = self.tree.identify_region(event.x, event.y)
        if region == "heading":
            # Ha a headerre kattintottak, ne csináljunk semmit
            return
        
        item_id = self.tree.identify_row(event.y)
        if not item_id:
            return
        tags = self.tree.item(item_id, 'tags')
        if 'subtitle' in tags:
            return
        for video_path, vid_item_id in self.video_items.items():
            if vid_item_id == item_id:
                output_file = self.video_to_output.get(video_path)
                if output_file and output_file.exists():
                    open_video_file(output_file)
                else:
                    messagebox.showinfo("Info", t('msg_video_not_exists'))
                break

    def on_right_click(self, event):
        """Handle right-click event on Treeview items.
        
        Shows a context menu with options for re-encoding, audio manipulation,
        and VMAF/PSNR testing.
        """

        item_id = self.tree.identify_row(event.y)
        if not item_id:
            return

        tags = self.tree.item(item_id, 'tags')
        if 'subtitle' in tags:
            return

        # Több videó kiválasztásának támogatása
        selected_items = self.tree.selection()
        if not selected_items:
            selected_items = [item_id]
        
        # Szűrjük ki a subtitle elemeket
        selected_video_items = []
        for sel_item in selected_items:
            sel_tags = self.tree.item(sel_item, 'tags')
            if 'subtitle' not in sel_tags:
                selected_video_items.append(sel_item)

        if not selected_video_items:
            return

        menu = tk.Menu(self.root, tearoff=0)

        # Ha több videó van kiválasztva, csak a VMAF opciót mutatjuk
        if len(selected_video_items) > 1:
            multi_completed = True
            for sel_item in selected_video_items:
                values = self.tree.item(sel_item, 'values')
                status = values[self.COLUMN_INDEX['status']] if len(values) > self.COLUMN_INDEX['status'] else ""
                video_path = self._get_video_path_by_item(sel_item)
                output_file = self.video_to_output.get(video_path) if video_path else None
                if not (video_path and is_status_completed(status) and output_file and output_file.exists()):
                    multi_completed = False
                    break

            reencode_label_key = 'context_multi_reencode_menu' if multi_completed else 'context_multi_encode_menu'
            reencode_menu = tk.Menu(menu, tearoff=0)
            reencode_menu.add_command(
                label=self._get_context_label('auto', multi_completed),
                command=lambda: self.bulk_schedule_auto(selected_video_items, 'auto')
            )
            reencode_menu.add_command(
                label=self._get_context_label('svt', multi_completed),
                command=lambda: self.bulk_schedule_auto(selected_video_items, 'svt')
            )
            reencode_menu.add_command(
                label=self._get_context_label('nvenc', multi_completed),
                state=tk.NORMAL if self.nvenc_enabled.get() else tk.DISABLED,
                command=lambda: self.bulk_schedule_auto(selected_video_items, 'nvenc')
            )
            menu.add_cascade(label=t(reencode_label_key), menu=reencode_menu)

            if multi_completed:
                menu.add_separator()
                menu.add_command(
                    label=t('menu_vmaf_test_multiple').format(count=len(selected_video_items)),
                    command=lambda: self.request_vmaf_test_multiple(selected_video_items)
                )
            if menu.index('end') is not None:
                menu.post(event.x_root, event.y_root)
            return

        # Egy videó kiválasztva - normál menü
        selected_video_path = None
        for video_path, vid_item_id in self.video_items.items():
            if vid_item_id == item_id:
                selected_video_path = video_path
                break

        if not selected_video_path:
            return

        values = self.tree.item(item_id, 'values')
        status = values[self.COLUMN_INDEX['status']] if len(values) > self.COLUMN_INDEX['status'] else ""
        cq_str = values[self.COLUMN_INDEX['cq']] if len(values) > self.COLUMN_INDEX['cq'] else ""

        is_completed = is_status_completed(status)
        output_file = self.video_to_output.get(selected_video_path)

        # Megnyitás submenü – elsőként jelenik meg
        open_submenu = tk.Menu(menu, tearoff=0)
        menu.add_cascade(label=t('menu_open'), menu=open_submenu)
        open_submenu.add_command(
            label=t('menu_source_video'),
            command=lambda: open_video_file(selected_video_path)
        )
        if output_file and output_file.exists():
            open_submenu.add_command(
                label=t('menu_encoded_video'),
                command=lambda: open_video_file(output_file)
            )

        auto_menu = tk.Menu(menu, tearoff=0)
        auto_menu.add_command(
            label=self._get_context_label('auto', is_completed),
            command=lambda: self.schedule_auto_encode(selected_video_path, item_id, 'auto')
        )
        auto_menu.add_command(
            label=self._get_context_label('nvenc', is_completed),
            state=tk.NORMAL if self.nvenc_enabled.get() else tk.DISABLED,
            command=lambda: self.schedule_auto_encode(selected_video_path, item_id, 'nvenc')
        )
        auto_menu.add_command(
            label=self._get_context_label('svt', is_completed),
            command=lambda: self.reencode_with_svt_av1(selected_video_path, item_id)
        )
        auto_label = t('context_auto_reencode') if is_completed else t('context_auto_encode')
        menu.add_cascade(label=auto_label, menu=auto_menu)

        # Újrakódolás submenü (kézi CQ csak ha kész és van CQ érték)
        current_cq = None
        if cq_str and cq_str != "-":
            try:
                current_cq = int(float(cq_str))
            except (ValueError, TypeError):
                current_cq = None
        if current_cq is None and output_file and output_file.exists():
            meta_info = get_output_file_info(output_file)
            if meta_info:
                meta_cq = meta_info[0]
                if meta_cq is not None:
                    try:
                        current_cq = int(meta_cq)
                    except (ValueError, TypeError):
                        current_cq = None

        if is_status_completed(status) and current_cq is not None:
            reencode_submenu = tk.Menu(menu, tearoff=0)
            menu.add_cascade(label=t('menu_reencode'), menu=reencode_submenu)
            
            encoder_type = "NVENC"
            if "(SVT-AV1)" in status:
                encoder_type = "SVT-AV1"
            elif "(NVENC)" in status:
                encoder_type = "NVENC"
            
            for offset in range(-5, 6):
                new_cq = current_cq + offset
                if new_cq == current_cq:
                    reencode_submenu.add_command(
                        label=f"{new_cq} (aktuális)",
                        state="disabled"
                    )
                else:
                    sign = "+" if offset > 0 else ""
                    reencode_submenu.add_command(
                        label=f"{sign}{offset}: CQ {new_cq}",
                        command=lambda cq=new_cq, vid=selected_video_path, itm=item_id, enc=encoder_type: 
                            self.reencode_with_cq(vid, itm, cq, enc)
                    )

        # VMAF ellenőrzés (csak ha kész és létezik az átkódolt fájl)
        if is_status_completed(status) and output_file and output_file.exists():
            audio_tracks = get_audio_stream_details(output_file)
            if audio_tracks:
                audio_menu = tk.Menu(menu, tearoff=0)
                for track in audio_tracks:
                    audio_menu.add_command(
                        label=f"{t('menu_audio_remove_action')}: {track['description']}",
                        command=lambda tr=track: self.confirm_audio_track_removal(selected_video_path, output_file, item_id, tr)
                    )
                menu.add_cascade(label=t('menu_audio_tracks'), menu=audio_menu)

                surround_tracks = [track for track in audio_tracks if track.get('channels', 0) >= 5]
                if surround_tracks:
                    convert_menu = tk.Menu(menu, tearoff=0)
                    method_options = [
                        ('fast', t('audio_compression_fast')),
                        ('dialogue', t('audio_compression_dialogue'))
                    ]
                    for track in surround_tracks:
                        track_submenu = tk.Menu(convert_menu, tearoff=0)
                        for method_key, method_label in method_options:
                            track_submenu.add_command(
                                label=method_label,
                                command=lambda tr=track, mk=method_key: self.confirm_audio_track_conversion(selected_video_path, output_file, item_id, tr, mk)
                            )
                        convert_menu.add_cascade(label=track['description'], menu=track_submenu)
                    menu.add_cascade(label=t('menu_audio_convert'), menu=convert_menu)

            menu.add_separator()
            vmaf_menu = tk.Menu(menu, tearoff=0)
            vmaf_menu.add_command(
                label=t('menu_vmaf_full'),
                command=lambda: self.request_vmaf_test(selected_video_path, item_id, check_vmaf=True, check_psnr=True)
            )
            vmaf_menu.add_command(
                label=t('menu_vmaf_only'),
                command=lambda: self.request_vmaf_test(selected_video_path, item_id, check_vmaf=True, check_psnr=False)
            )
            vmaf_menu.add_command(
                label=t('menu_psnr_only'),
                command=lambda: self.request_vmaf_test(selected_video_path, item_id, check_vmaf=False, check_psnr=True)
            )
            menu.add_cascade(label=t('menu_vmaf_full'), menu=vmaf_menu)

        if menu.index('end') is not None:
            menu.post(event.x_root, event.y_root)

    def request_vmaf_test_multiple(self, item_ids):
        """VMAF/PSNR teszt kérése több videóra"""
        for item_id in item_ids:
            video_path = None
            for vid_path, vid_item_id in self.video_items.items():
                if vid_item_id == item_id:
                    video_path = vid_path
                    break
            
            if video_path:
                self.request_vmaf_test(video_path, item_id)

    def ensure_vmaf_worker_running(self):
        """Gondoskodik róla, hogy a VMAF/PSNR worker és a queue feldolgozás aktív legyen."""
        if STOP_EVENT.is_set():
            STOP_EVENT.clear()
        if not hasattr(self, 'vmaf_thread') or not self.vmaf_thread.is_alive():
            self.vmaf_worker_active = True
            self.vmaf_thread = threading.Thread(target=self.vmaf_worker, daemon=True)
            self.vmaf_thread.start()
        self.immediate_stop_button.config(state=tk.NORMAL)
        if not self.is_encoding:
            self.is_encoding = True
            self.start_button.config(text=t('btn_stop'), command=self.stop_encoding_graceful, state=tk.NORMAL)
            self.check_encoding_queue()

    def request_vmaf_test(self, video_path, item_id, check_vmaf=True, check_psnr=True):
        """Request VMAF/PSNR calculation for a specific video.
        
        Adds the request to the VMAF worker queue.
        
        Args:
            video_path: Path to the video file.
            item_id: Treeview item ID.
            check_vmaf: Whether to calculate VMAF.
            check_psnr: Whether to calculate PSNR.
        """

        output_file = self.video_to_output.get(video_path)
        if not output_file or not output_file.exists():
            messagebox.showerror("Hiba", t('msg_output_not_found'))
            return
        
        if not check_vmaf and not check_psnr:
            check_vmaf = True  # Biztonsági okból legalább az egyiket futtatjuk
        
        # KRITIKUS: Ha STOP_EVENT be van állítva (az előző azonnali leállítás miatt), töröljük
        # Mert különben a VMAF/PSNR worker azonnal kilép
        if STOP_EVENT.is_set():
            STOP_EVENT.clear()
        
        # VMAF/PSNR queue-ba helyezés
        vmaf_task = {
            'video_path': video_path,
            'output_file': output_file,
            'item_id': item_id,
            'orig_size_str': self.tree.item(item_id, 'values')[6] if len(self.tree.item(item_id, 'values')) > 6 else "-",
            'check_vmaf': bool(check_vmaf),
            'check_psnr': bool(check_psnr),
        }
        VMAF_QUEUE.put(vmaf_task)
        
        # Státusz frissítés
        current_values = self.tree.item(item_id, 'values')
        cq_str = current_values[self.COLUMN_INDEX['cq']] if len(current_values) > self.COLUMN_INDEX['cq'] else "-"
        vmaf_str = current_values[self.COLUMN_INDEX['vmaf']] if len(current_values) > self.COLUMN_INDEX['vmaf'] else "-"
        psnr_str = current_values[self.COLUMN_INDEX['psnr']] if len(current_values) > self.COLUMN_INDEX['psnr'] else "-"
        progress_str = current_values[self.COLUMN_INDEX['progress']] if len(current_values) > self.COLUMN_INDEX['progress'] else "-"
        orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
        new_size_str = current_values[self.COLUMN_INDEX['new_size']] if len(current_values) > self.COLUMN_INDEX['new_size'] else "-"
        change_str = current_values[self.COLUMN_INDEX['size_change']] if len(current_values) > self.COLUMN_INDEX['size_change'] else "-"
        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
        waiting_status = self._get_vmaf_waiting_status_text(vmaf_task['check_vmaf'], vmaf_task['check_psnr'])
        self.encoding_queue.put(("update", item_id, waiting_status, cq_str, vmaf_str, psnr_str, progress_str, orig_size_str, new_size_str, change_str, completed_date))
        self.encoding_queue.put(("tag", item_id, "pending"))
        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
        self.ensure_vmaf_worker_running()

    def _get_vmaf_waiting_status_text(self, check_vmaf, check_psnr):
        if check_vmaf and check_psnr:
            return t('status_vmaf_psnr_waiting')
        if check_vmaf:
            return t('status_vmaf_waiting')
        return t('status_psnr_waiting')

    def confirm_audio_track_removal(self, video_path, output_file, item_id, track_info):
        """Hangsáv eltávolításának megerősítése és ütemezése."""
        if not output_file or not output_file.exists():
            messagebox.showerror("Hiba", t('msg_output_not_found'))
            return

        description = track_info.get('description', 'Audio')
        confirmation = messagebox.askyesno(
            t('menu_audio_tracks'),
            f"{t('menu_audio_remove_confirm')}\n\n{description}"
        )
        if not confirmation:
            return

        self.schedule_audio_track_removal(video_path, output_file, item_id, track_info)

    def _capture_audio_task_state(self, item_id):
        """Visszaadja a kiválasztott sor eredeti állapotát a hangsáv műveletekhez."""
        current_values = self.get_tree_values(item_id, min_length=len(self.COLUMN_INDEX))
        status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
        cq_str = current_values[self.COLUMN_INDEX['cq']] if len(current_values) > self.COLUMN_INDEX['cq'] else "-"
        vmaf_str = current_values[self.COLUMN_INDEX['vmaf']] if len(current_values) > self.COLUMN_INDEX['vmaf'] else "-"
        psnr_str = current_values[self.COLUMN_INDEX['psnr']] if len(current_values) > self.COLUMN_INDEX['psnr'] else "-"
        progress_str = current_values[self.COLUMN_INDEX['progress']] if len(current_values) > self.COLUMN_INDEX['progress'] else "-"
        orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
        new_size_str = current_values[self.COLUMN_INDEX['new_size']] if len(current_values) > self.COLUMN_INDEX['new_size'] else "-"
        change_str = current_values[self.COLUMN_INDEX['size_change']] if len(current_values) > self.COLUMN_INDEX['size_change'] else "-"
        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
        return {
            'status': status,
            'cq': cq_str,
            'vmaf': vmaf_str,
            'psnr': psnr_str,
            'progress': progress_str,
            'orig_size': orig_size_str,
            'new_size': new_size_str,
            'change': change_str,
            'completed_date': completed_date,
            'tag': 'completed' if is_status_completed(status) else 'pending'
        }
    
    def _get_audio_method_label(self, method_key):
        method = (method_key or 'fast').lower()
        return t('audio_compression_dialogue') if method == 'dialogue' else t('audio_compression_fast')

    def schedule_audio_track_removal(self, video_path, output_file, item_id, track_info):
        """Feladat ütemezése a CPU workerre egy hangsáv eltávolításához."""
        video_path = Path(video_path)
        output_file = Path(output_file)
        original_info = self._capture_audio_task_state(item_id)
        cq_str = original_info['cq']
        vmaf_str = original_info['vmaf']
        psnr_str = original_info['psnr']
        orig_size_str = original_info['orig_size']
        new_size_str = original_info['new_size']
        change_str = original_info['change']
        completed_date = original_info['completed_date']

        self.audio_edit_task_info[item_id] = original_info

        task = {
            'video_path': video_path,
            'output_file': output_file,
            'item_id': item_id,
            'track_info': track_info,
            'original': original_info,
            'action': 'remove'
        }
        AUDIO_EDIT_QUEUE.put(task)

        self.encoding_queue.put(("update", item_id, t('status_audio_edit_queue'), cq_str, vmaf_str, psnr_str, "-", orig_size_str, new_size_str, change_str, completed_date))
        self.encoding_queue.put(("tag", item_id, "pending"))
        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik

        if not self.is_encoding:
            self.is_encoding = True
            self.audio_edit_only_mode = True
            self.start_button.config(text=t('btn_stop'), command=self.stop_encoding_graceful, state=tk.NORMAL)
            self.immediate_stop_button.config(state=tk.NORMAL)
            self.load_videos_btn.config(state=tk.DISABLED)
            self.root.after(100, self.check_encoding_queue)

        if not self.audio_edit_thread or not self.audio_edit_thread.is_alive():
            self.audio_edit_thread = threading.Thread(target=self.audio_edit_worker, daemon=True)
        description = track_info.get('description', 'Audio')
        prompt = t('menu_audio_convert_confirm').format(track=description, method=method_label)
        if not messagebox.askyesno(t('menu_audio_convert'), prompt):
            return

        self.schedule_audio_track_conversion(video_path, output_file, item_id, track_info, method_key)

    def schedule_audio_track_conversion(self, video_path, output_file, item_id, track_info, method_key):
        """Feladat ütemezése térhatású hangsáv 2.0 konverziójára."""
        video_path = Path(video_path)
        output_file = Path(output_file)
        original_info = self._capture_audio_task_state(item_id)
        cq_str = original_info['cq']
        vmaf_str = original_info['vmaf']
        psnr_str = original_info['psnr']
        orig_size_str = original_info['orig_size']
        new_size_str = original_info['new_size']
        change_str = original_info['change']
        completed_date = original_info['completed_date']

        self.audio_edit_task_info[item_id] = original_info

        task = {
            'video_path': video_path,
            'output_file': output_file,
            'item_id': item_id,
            'track_info': track_info,
            'original': original_info,
            'action': 'convert',
            'conversion_method': method_key
        }
        AUDIO_EDIT_QUEUE.put(task)

        self.encoding_queue.put(("update", item_id, t('status_audio_edit_queue'), cq_str, vmaf_str, psnr_str, "-", orig_size_str, new_size_str, change_str, completed_date))
        self.encoding_queue.put(("tag", item_id, "pending"))
        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik

        if not self.is_encoding:
            self.is_encoding = True
            self.audio_edit_only_mode = True
            self.start_button.config(text=t('btn_stop'), command=self.stop_encoding_graceful, state=tk.NORMAL)
            self.immediate_stop_button.config(state=tk.NORMAL)
            self.load_videos_btn.config(state=tk.DISABLED)
            self.root.after(100, self.check_encoding_queue)

        if not self.audio_edit_thread or not self.audio_edit_thread.is_alive():
            self.audio_edit_thread = threading.Thread(target=self.audio_edit_worker, daemon=True)
            self.audio_edit_thread.start()
        
        self.update_start_button_state()

    def audio_edit_worker(self):
        """Background worker for audio manipulation tasks.
        
        Handles removing audio tracks and converting to stereo.
        """

        set_low_priority()
        task_in_progress = False
        while True:
            if STOP_EVENT.is_set():
                self._reset_audio_tasks_pending()
                break
            if self.graceful_stop_requested and not task_in_progress:
                break
            try:
                task = AUDIO_EDIT_QUEUE.get(timeout=1)
            except queue.Empty:
                if STOP_EVENT.is_set():
                    break
                if self.graceful_stop_requested:
                    break
                if AUDIO_EDIT_QUEUE.empty():
                    break
                continue

            item_id = task.get('item_id')
            original = task.get('original') or self.audio_edit_task_info.get(item_id)
            task_in_progress = True
            try:
                self._process_audio_edit_task(task)
            except EncodingStopped:
                self._restore_audio_task_state(item_id, original)
                self.audio_edit_task_info.pop(item_id, None)
                AUDIO_EDIT_QUEUE.task_done()
                task_in_progress = False
                break
            except Exception as e:
                with console_redirect(self.svt_logger):
                    print(f"\n✗ Hangsáv eltávolítás hiba: {e}\n")
                if original:
                    failure_status = t('status_audio_edit_failed')
                    self.encoding_queue.put(("update", item_id, failure_status, original['cq'], original['vmaf'], original['psnr'], "-", original['orig_size'], original['new_size'], original['change'], original['completed_date']))
                    self.encoding_queue.put(("tag", item_id, "failed"))
                    # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                self.audio_edit_task_info.pop(item_id, None)
                AUDIO_EDIT_QUEUE.task_done()
                task_in_progress = False
                continue

            self.audio_edit_task_info.pop(item_id, None)
            AUDIO_EDIT_QUEUE.task_done()
            task_in_progress = False

        self.root.after(0, self._on_audio_edit_worker_finished)

    def _process_audio_edit_task(self, task):
        """Audio feladat feldolgozása (eltávolítás vagy konverzió)."""
        action = (task.get('action') or 'remove').lower()
        if action == 'convert':
            self._process_audio_conversion_task(task)
        else:
            self._process_audio_removal_task(task)

    def _process_audio_removal_task(self, task):
        """Egyetlen hangsáv eltávolításának végrehajtása."""
        video_path = task['video_path']
        output_file = task['output_file']
        item_id = task['item_id']
        track_info = task['track_info']
        original = task['original']

        self.encoding_queue.put(("update", item_id, t('status_audio_editing'), original['cq'], original['vmaf'], original['psnr'], "-", original['orig_size'], original['new_size'], original['change'], original['completed_date']))
        self.encoding_queue.put(("tag", item_id, "audio_edit"))
        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik

        with CPU_WORKER_LOCK:
            with console_redirect(self.svt_logger):
                print(f"\n{'='*80}\nHANGSÁV ELTÁVOLÍTÁS: {output_file.name}\n{track_info.get('description', '')}\n{'='*80}\n")
            remove_audio_track_from_file(output_file, track_info['ffmpeg_audio_index'], logger=self.svt_logger, stop_event=STOP_EVENT)

        orig_size_mb, new_size_mb, change_percent = self.calculate_file_sizes(video_path, output_file)
        orig_size_str = f"{format_localized_number(orig_size_mb, decimals=1)} MB" if orig_size_mb else original['orig_size']
        new_size_str = f"{format_localized_number(new_size_mb, decimals=1)} MB"
        change_str = f"{format_localized_number(change_percent, decimals=2, show_sign=True)}%" if orig_size_mb else original['change']
        completed_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        self.encoding_queue.put(("update", item_id, t('status_audio_edit_done'), original['cq'], original['vmaf'], original['psnr'], "100%", orig_size_str, new_size_str, change_str, completed_date))
        self.encoding_queue.put(("tag", item_id, "completed"))
        self.encoding_queue.put(("update_summary",))
        
        # Adatbázis frissítése hangsáv módosítás befejezése után
        if video_path:
            def update_db_after_audio_edit():
                try:
                    self.update_single_video_in_db(
                        video_path, item_id, t('status_audio_edit_done'), 
                        original['cq'], original['vmaf'], original['psnr'], 
                        orig_size_str, new_size_mb, change_percent, completed_date
                    )
                except Exception as e:
                    # Csendes hiba - ne zavarjuk meg az audio edit folyamatot
                    if LOG_WRITER:
                        try:
                            LOG_WRITER.write(f"⚠ [audio_edit] Adatbázis frissítés hiba: {e} | video: {video_path}\n")
                            LOG_WRITER.flush()
                        except Exception:
                            pass
            
            db_thread = threading.Thread(target=update_db_after_audio_edit, daemon=True)
            db_thread.start()

        with console_redirect(self.svt_logger):
            print(f"\n✓ Hangsáv eltávolítva: {output_file.name} -> {track_info.get('description', '')}\n")
    
    def _process_audio_conversion_task(self, task):
        """Új 2.0 hangsáv létrehozása egy kiválasztott térhatású sávból."""
        video_path = task['video_path']
        output_file = task['output_file']
        item_id = task['item_id']
        track_info = task['track_info']
        original = task['original']
        method_key = task.get('conversion_method', 'fast')
        if not output_file.exists():
            raise FileNotFoundError("Kimeneti fájl nem található a hangsáv konverzióhoz.")

        self.encoding_queue.put(("update", item_id, t('status_audio_editing'), original['cq'], original['vmaf'], original['psnr'], "-", original['orig_size'], original['new_size'], original['change'], original['completed_date']))
        self.encoding_queue.put(("tag", item_id, "audio_edit"))
        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik

        audio_index = track_info.get('ffmpeg_audio_index')
        language_norm = track_info.get('language_normalized')
        convert_audio_track_to_stereo(output_file, audio_index, method=method_key, language_code=language_norm, logger=self.svt_logger, stop_event=STOP_EVENT)

        orig_size_mb, new_size_mb, change_percent = self.calculate_file_sizes(video_path, output_file)
        orig_size_str = f"{format_localized_number(orig_size_mb, decimals=1)} MB" if orig_size_mb else original['orig_size']
        new_size_str = f"{format_localized_number(new_size_mb, decimals=1)} MB" if new_size_mb else original['new_size']
        change_str = f"{format_localized_number(change_percent, decimals=2, show_sign=True)}%" if orig_size_mb else original['change']
        completed_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        self.encoding_queue.put(("update", item_id, t('status_audio_edit_done'), original['cq'], original['vmaf'], original['psnr'], "100%", orig_size_str, new_size_str, change_str, completed_date))
        self.encoding_queue.put(("tag", item_id, "completed"))
        self.encoding_queue.put(("update_summary",))
        
        # Adatbázis frissítése hangsáv konverzió befejezése után
        if video_path:
            def update_db_after_audio_conversion():
                try:
                    self.update_single_video_in_db(
                        video_path, item_id, t('status_audio_edit_done'), 
                        original['cq'], original['vmaf'], original['psnr'], 
                        orig_size_str, new_size_mb, change_percent, completed_date
                    )
                except Exception as e:
                    # Csendes hiba - ne zavarjuk meg az audio conversion folyamatot
                    if LOG_WRITER:
                        try:
                            LOG_WRITER.write(f"⚠ [audio_conversion] Adatbázis frissítés hiba: {e} | video: {video_path}\n")
                            LOG_WRITER.flush()
                        except Exception:
                            pass
            
            db_thread = threading.Thread(target=update_db_after_audio_conversion, daemon=True)
            db_thread.start()

        method_label = self._get_audio_method_label(method_key)
        with console_redirect(self.svt_logger):
            print(f"\n✓ Új 2.0 hangsáv hozzáadva ({method_label}): {output_file.name} - {track_info.get('description', '')}\n")

    def _restore_audio_task_state(self, item_id, original):
        """Visszaállítja az eredeti státuszt (stop vagy hiba esetén)."""
        if not original:
            return
        status = original.get('status', t('status_ready'))
        tag = original.get('tag', 'pending')
        progress = original.get('progress', "-")
        self.encoding_queue.put(("update", item_id, status, original['cq'], original['vmaf'], original['psnr'], progress, original['orig_size'], original['new_size'], original['change'], original['completed_date']))
        self.encoding_queue.put(("tag", item_id, tag))
        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik

    def _reset_audio_tasks_pending(self):
        """Az audio queue-ban váró feladatok státuszát visszaállítja (stop esetén)."""
        while True:
            try:
                task = AUDIO_EDIT_QUEUE.get_nowait()
            except queue.Empty:
                break
            item_id = task.get('item_id')
            original = task.get('original')
            self._restore_audio_task_state(item_id, original)
            self.audio_edit_task_info.pop(item_id, None)
            AUDIO_EDIT_QUEUE.task_done()

    def _on_audio_edit_worker_finished(self):
        """Audio worker leállásakor visszaállítja a gombokat, ha csak az futott."""
        self.audio_edit_thread = None
        if self.audio_edit_only_mode:
            self.audio_edit_only_mode = False
            self.is_encoding = False
            self.start_button.config(text=t('btn_start'), command=self.start_encoding, state=tk.NORMAL)
            self.immediate_stop_button.config(state=tk.DISABLED)
            self.load_videos_btn.config(state=tk.NORMAL)
            self.status_label.config(text=t('status_ready'))
        self.update_start_button_state()
        self._reset_encoding_ui_if_idle()

    def reencode_with_svt_av1(self, video_path, item_id, prompt=True):
        """Initiate SVT-AV1 re-encoding for a video.
        
        Args:
            video_path: Path to the video file.
            item_id: Treeview item ID.
            prompt: If True, asks for confirmation before starting.
        """

        current_values = self.tree.item(item_id, 'values')
        status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""

        if "SVT-AV1" in status and "queue" in status.lower():
            messagebox.showwarning("Figyelem", t('msg_svt_already_processing'))
            return False

        if prompt:
            result = messagebox.askyesno(
                t('menu_reencode_svt'),
                f"{t('msg_svt_reencode_confirm')}\n\n{video_path.name}\n\nEz felülírja a meglévő fájlt!"
            )
            if not result:
                return False

        if not hasattr(self, 'svt_thread') or not self.svt_thread.is_alive():
            self.svt_thread = threading.Thread(target=self.svt_worker, daemon=True)
            self.svt_thread.start()

        output_file = self.video_to_output.get(video_path)
        if not output_file:
            messagebox.showerror("Hiba", t('msg_file_info_missing'))
            return

        if output_file.exists():
            try:
                output_file.unlink()
            except Exception as e:
                if prompt:
                    messagebox.showerror("Hiba", f"{t('msg_delete_failed')}\n{e}")
                return False

        valid_subtitles, invalid_subtitles = self._get_validated_subtitles_for_video(video_path)
        subtitle_files = valid_subtitles
        orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"

        svt_task = {
            'video_path': video_path,
            'output_file': output_file,
            'subtitle_files': subtitle_files,
            'invalid_subtitles': invalid_subtitles,
            'item_id': item_id,
            'orig_size_str': orig_size_str,
            'initial_min_vmaf': self.min_vmaf.get(),
            'vmaf_step': self.vmaf_step.get(),
            'max_encoded': self.max_encoded_percent.get(),
            'resize_enabled': self.resize_enabled.get(),
            'resize_height': self.resize_height.get(),
            'audio_compression_enabled': self.audio_compression_enabled.get(),
            'audio_compression_method': self.audio_compression_method.get(),
            'reason': 'manual_reencode'
        }

        SVT_QUEUE.put(svt_task)

        current_values = self.tree.item(item_id, 'values')
        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
        status_text = t('status_svt_queue')
        # Azonnali frissítés a tree-ben (UI thread)
        new_values = list(current_values)
        if len(new_values) < len(self.COLUMN_INDEX):
            new_values.extend([''] * (len(self.COLUMN_INDEX) - len(new_values)))
        new_values[self.COLUMN_INDEX['status']] = status_text
        new_values[self.COLUMN_INDEX['cq']] = "-"
        new_values[self.COLUMN_INDEX['vmaf']] = "-"
        new_values[self.COLUMN_INDEX['psnr']] = "-"
        new_values[self.COLUMN_INDEX['progress']] = "-"
        new_values[self.COLUMN_INDEX['new_size']] = "-"
        new_values[self.COLUMN_INDEX['size_change']] = "-"
        # Megtartjuk a duration és frames értékeket
        new_values[self.COLUMN_INDEX['completed_date']] = completed_date
        self.tree.item(item_id, values=tuple(new_values))

        self.encoding_queue.put(("update", item_id, status_text, "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
        self.encoding_queue.put(("tag", item_id, "encoding_svt"))
        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik  # JSON mentés SVT queue-ba kerülés után

        # Start gomb állapotának frissítése
        self.update_start_button_state()

        if prompt:
            messagebox.showinfo("Indítva", f"{t('msg_svt_added')}\n{video_path.name}")
        else:
            self.log_status(f"✓ SVT-AV1 sorba állítva (batch): {video_path.name}")
        return True
    
    def reencode_with_cq(self, video_path, item_id, target_cq, encoder_type):
        """Initiate re-encoding with a specific CQ/CRF value.
        
        Args:
            video_path: Path to the video file.
            item_id: Treeview item ID.
            target_cq: Target CQ/CRF value.
            encoder_type: 'nvenc' or 'svt-av1'.
        """

        output_file = self.video_to_output.get(video_path)
        if not output_file:
            messagebox.showerror("Hiba", t('msg_file_info_missing'))
            return
        
        current_values = self.tree.item(item_id, 'values')
        orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
        vmaf_str = current_values[self.COLUMN_INDEX['vmaf']] if len(current_values) > self.COLUMN_INDEX['vmaf'] else None
        
        # Kérdezzük meg a felhasználót
        encoder_display = "NVENC" if encoder_type == "NVENC" else "SVT-AV1"
        result = messagebox.askyesno(
            t('menu_reencode'),
            f"{t('msg_reencode_confirm')}\n\n"
            f"Videó: {video_path.name}\n"
            f"Encoder: {encoder_display}\n"
            f"CQ/CRF: {target_cq}\n\n"
            f"Ez felülírja a meglévő fájlt!"
        )
        
        if not result:
            return
        
        # Fájl törlése ha létezik
        if output_file.exists():
            try:
                output_file.unlink()
            except Exception as e:
                messagebox.showerror("Hiba", f"{t('msg_delete_failed')}\n{e}")
                return
        
        valid_subtitles, invalid_subtitles = self._get_validated_subtitles_for_video(video_path)
        subtitle_files = valid_subtitles
        
        # VMAF érték konvertálása ha elérhető
        vmaf_value = None
        if vmaf_str and vmaf_str != "-":
            try:
                vmaf_value = float(vmaf_str)
            except (ValueError, TypeError):
                vmaf_value = None
        
        if encoder_type == "NVENC":
            # NVENC encoding queue-ba - manuális task-ként
            task = {
                'video_path': video_path,
                'output_file': output_file,
                'subtitle_files': subtitle_files,
                'invalid_subtitles': invalid_subtitles,
                'item_id': item_id,
                'orig_size_str': orig_size_str,
                'target_cq': target_cq,
                'vmaf_value': vmaf_value,
                'resize_enabled': self.resize_enabled.get(),
                'resize_height': self.resize_height.get(),
                'skip_crf_search': True,
                'reason': 'manual_reencode_cq'
            }
            # Hozzáadás a manuális task listához és worker indítása
            self.manual_nvenc_tasks.append(task)
            self.encoding_queue.put(("update", item_id, t('status_nvenc_queue') + " (CQ újrakódolás)", "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
            self.encoding_queue.put(("tag", item_id, "pending"))
            if not self.is_encoding:
                STOP_EVENT.clear()
                self.is_encoding = True
                self.start_button.config(text=t('btn_stop'), command=self.stop_encoding_graceful, state=tk.NORMAL)
                self.immediate_stop_button.config(state=tk.NORMAL)
                self.load_videos_btn.config(state=tk.DISABLED)
                self.root.after(100, self.check_encoding_queue)
            # Worker thread indítása, ha nincs futó
            if not hasattr(self, 'manual_nvenc_worker') or not self.manual_nvenc_worker.is_alive():
                self.manual_nvenc_worker = threading.Thread(target=self.process_manual_nvenc_tasks_worker, daemon=True)
                self.manual_nvenc_worker.start()
        else:
            # SVT-AV1 encoding queue-ba
            task = {
                'video_path': video_path,
                'output_file': output_file,
                'subtitle_files': subtitle_files,
                'invalid_subtitles': invalid_subtitles,
                'item_id': item_id,
                'orig_size_str': orig_size_str,
                'initial_min_vmaf': self.min_vmaf.get(),
                'vmaf_step': self.vmaf_step.get(),
                'max_encoded': self.max_encoded_percent.get(),
                'resize_enabled': self.resize_enabled.get(),
                'resize_height': self.resize_height.get(),
                'target_cq': target_cq,
                'vmaf_value': vmaf_value,
                'skip_crf_search': True,
                'reason': 'manual_reencode_cq'
            }
            SVT_QUEUE.put(task)
            self.encoding_queue.put(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
            self.encoding_queue.put(("tag", item_id, "encoding_svt"))
        
        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
        messagebox.showinfo("Hozzáadva", f"{t('msg_reencode_added').format(encoder=encoder_display)}\n{video_path.name}\nCQ/CRF: {target_cq}")
    
    def on_column_resize(self, event):
        tree = event.widget
        region = tree.identify_region(event.x, event.y)
        if region == "separator":
            self.root.after(10, self.sync_column_widths)
    
    def sync_column_widths(self):
        for col in ['#0'] + list(self.tree['columns']):
            width = self.tree.column(col, 'width')
            self.col_widths[col] = width
            # Balra igazítás megőrzése a fájlméret oszlopoknál
            if col in ("orig_size", "new_size", "size_change"):
                self.summary_tree.column(col, width=width, anchor=tk.W)
            else:
                self.summary_tree.column(col, width=width)
    
    def _sort_tree_by_order_num(self):
        """Rendezi a TreeView elemeit order_num szerint (ABC sorrend)"""
        try:
            # Összes fő elem lekérése (gyerekeket nem rendezzük külön)
            items = []
            for item_id in self.tree.get_children():
                tags = self.tree.item(item_id, 'tags')
                if 'subtitle' not in tags:  # Csak videókat rendezünk, subtitle-eket nem
                    items.append(item_id)
            
            # Rendezés order_num szerint (a text mező tartalmazza az order_num-ot)
            def get_order_key(item_id):
                try:
                    text = self.tree.item(item_id, 'text')
                    return int(text) if text.isdigit() else 999999
                except (ValueError, TypeError, tk.TclError):
                    return 999999
            
            items.sort(key=get_order_key)
            
            # Újra beszúrás rendezett sorrendben
            for item_id in items:
                self.tree.move(item_id, "", tk.END)
        except Exception as e:
            # Csendes hiba, nem logoljuk, mert ez csak egy rendezés
            pass
    
    def sort_by_column(self, column):
        """Oszlop szerinti rendezés (A-Z / Z-A váltogatás)"""
        # Ha ugyanarra az oszlopra kattintottak, fordítjuk a rendezést
        if self.sort_column == column:
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_column = column
            self.sort_reverse = False
        
        # Rendezési kulcs függvény
        def get_sort_key(item_id):
            video_path = None
            for vp, vid_id in self.video_items.items():
                if vid_id == item_id:
                    video_path = vp
                    break
            
            if not video_path:
                return (999999,) if column == "#0" else ("", 999999)
            
            order_num = self.video_order.get(video_path, 999999)
            
            if column == "#0":
                # Sorszám szerinti rendezés - csak sorszám számít
                return (order_num,)
            
            # Más oszlopok szerinti rendezés - elsődleges az oszlop, másodlagos a sorszám
            values = self.tree.item(item_id, 'values')
            
            if column == "video_name":
                video_name = values[self.COLUMN_INDEX['video_name']] if len(values) > self.COLUMN_INDEX['video_name'] else ""
                return (video_name.lower() if video_name else "", order_num)
            elif column == "status":
                status = values[self.COLUMN_INDEX['status']] if len(values) > self.COLUMN_INDEX['status'] else ""
                return (status.lower(), order_num)
            elif column == "cq":
                cq = values[self.COLUMN_INDEX['cq']] if len(values) > self.COLUMN_INDEX['cq'] else ""
                try:
                    # Lokalizált szám kezelése
                    cq_normalized = normalize_number_string(cq) if cq != "-" else "-"
                    cq_val = float(cq_normalized) if cq_normalized != "-" else 999999
                except (ValueError, TypeError):
                    cq_val = 999999
                return (cq_val, order_num)
            elif column == "vmaf":
                vmaf = values[self.COLUMN_INDEX['vmaf']] if len(values) > self.COLUMN_INDEX['vmaf'] else ""
                try:
                    # Lokalizált szám kezelése
                    vmaf_normalized = normalize_number_string(vmaf) if vmaf != "-" else "-"
                    vmaf_val = float(vmaf_normalized) if vmaf_normalized != "-" else 0
                except (ValueError, TypeError):
                    vmaf_val = 0
                return (vmaf_val, order_num)
            elif column == "psnr":
                psnr = values[self.COLUMN_INDEX['psnr']] if len(values) > self.COLUMN_INDEX['psnr'] else ""
                try:
                    # Lokalizált szám kezelése
                    psnr_normalized = normalize_number_string(psnr) if psnr != "-" else "-"
                    psnr_val = float(psnr_normalized) if psnr_normalized != "-" else 0
                except (ValueError, TypeError):
                    psnr_val = 0
                return (psnr_val, order_num)
            elif column == "progress":
                progress = values[self.COLUMN_INDEX['progress']] if len(values) > self.COLUMN_INDEX['progress'] else ""
                return (progress.lower(), order_num)
            elif column == "orig_size":
                orig_size = values[self.COLUMN_INDEX['orig_size']] if len(values) > self.COLUMN_INDEX['orig_size'] else ""
                try:
                    # Lokalizált szám kezelése - kivonjuk a számot az "XXX.X MB" formátumból
                    if orig_size != "-":
                        size_str = orig_size.replace(" MB", "").strip()
                        size_normalized = normalize_number_string(size_str)
                        size_val = float(size_normalized) if size_normalized != "-" else 0
                    else:
                        size_val = 0
                except (ValueError, TypeError, AttributeError):
                    size_val = 0
                return (size_val, order_num)
            elif column == "new_size":
                new_size = values[self.COLUMN_INDEX['new_size']] if len(values) > self.COLUMN_INDEX['new_size'] else ""
                try:
                    # Lokalizált szám kezelése - kivonjuk a számot az "XXX.X MB" formátumból
                    if new_size != "-":
                        size_str = new_size.replace(" MB", "").strip()
                        size_normalized = normalize_number_string(size_str)
                        size_val = float(size_normalized) if size_normalized != "-" else 0
                    else:
                        size_val = 0
                except (ValueError, TypeError, AttributeError):
                    size_val = 0
                return (size_val, order_num)
            elif column == "size_change":
                size_change = values[self.COLUMN_INDEX['size_change']] if len(values) > self.COLUMN_INDEX['size_change'] else ""
                try:
                    # Lokalizált szám kezelése - kivonjuk a számot a "±XX.X%" formátumból
                    if size_change != "-":
                        # Eltávolítjuk a % jelet és a +/- jeleket
                        clean_val = size_change.replace("%", "").replace("+", "").strip()
                        change_normalized = normalize_number_string(clean_val)
                        change_val = float(change_normalized) if change_normalized != "-" else 0
                    else:
                        change_val = 0
                except (ValueError, TypeError, AttributeError):
                    change_val = 0
                return (change_val, order_num)
            elif column == "duration":
                duration = values[self.COLUMN_INDEX['duration']] if len(values) > self.COLUMN_INDEX['duration'] else ""
                # Időtartam formátum: "HH:MM:SS" - konvertáljuk másodpercekre
                if duration != "-" and duration:
                    try:
                        parts = duration.split(":")
                        if len(parts) == 3:
                            hours, minutes, seconds = map(int, parts)
                            duration_seconds = hours * 3600 + minutes * 60 + seconds
                        else:
                            duration_seconds = 0
                    except (ValueError, TypeError, AttributeError):
                        duration_seconds = 0
                else:
                    duration_seconds = 0
                return (duration_seconds, order_num)
            elif column == "frames":
                frames = values[self.COLUMN_INDEX['frames']] if len(values) > self.COLUMN_INDEX['frames'] else ""
                try:
                    # Lokalizált szám kezelése
                    if frames != "-" and frames:
                        frames_normalized = normalize_number_string(frames)
                        frames_val = int(float(frames_normalized)) if frames_normalized != "-" else 0
                    else:
                        frames_val = 0
                except (ValueError, TypeError, AttributeError):
                    frames_val = 0
                return (frames_val, order_num)
            elif column == "completed_date":
                completed_date = values[self.COLUMN_INDEX['completed_date']] if len(values) > self.COLUMN_INDEX['completed_date'] else ""
                return (completed_date.lower(), order_num)
            
            return ("", order_num)
        
        # Összes fő elem lekérése (gyerekeket nem rendezzük külön)
        items = []
        for item_id in self.tree.get_children():
            tags = self.tree.item(item_id, 'tags')
            if 'subtitle' not in tags:  # Csak videókat rendezünk, subtitle-eket nem
                items.append(item_id)
        
        # Rendezés
        items.sort(key=get_sort_key, reverse=self.sort_reverse)
        
        # Újra beszúrás rendezett sorrendben
        for item_id in items:
            self.tree.move(item_id, "", tk.END)
        
        # Header frissítése (nyil jelzés) - oszlopnevek megmaradnak, csak nyilat adunk hozzá
        for col in ['#0'] + list(self.tree['columns']):
            if col == column:
                arrow = " ↓" if self.sort_reverse else " ↑"
            else:
                arrow = ""
            # Oszlopnevek lekérése a t() függvényből, hogy lokalizáltak maradjanak
            heading_text_map = {
                "#0": t('column_order'),
                "video_name": t('column_video'),
                "status": t('column_status'),
                "cq": t('column_cq'),
                "vmaf": t('column_vmaf'),
                "psnr": t('column_psnr'),
                "progress": t('column_progress'),
                "orig_size": t('column_orig_size'),
                "new_size": t('column_new_size'),
                "size_change": t('column_size_change'),
                "duration": "Időtartam",  # Nincs fordítás, hardcoded
                "frames": "Frame-ek",  # Nincs fordítás, hardcoded
                "completed_date": t('column_completed')
            }
            heading_text = heading_text_map.get(col, "")
            self.tree.heading(col, text=heading_text + arrow)
        
    def browse_source(self):
        """Open directory browser for source folder selection."""
        folder = filedialog.askdirectory(title="Forrás mappa")
        if folder:
            self.source_entry.delete(0, tk.END)
            self.source_entry.insert(0, folder)

    def _on_tool_path_change(self, *args):
        self.apply_tool_paths_from_gui()

    def apply_tool_paths_from_gui(self):
        apply_external_tool_paths(
            (self.ffmpeg_path.get().strip() or None),
            (self.abav1_path.get().strip() or None),
            (self.virtualdub_path.get().strip() or None)
        )
    
    def browse_dest(self):
        """Open directory browser for destination folder selection."""
        folder = filedialog.askdirectory(title="Cél mappa")
        if folder:
            self.dest_entry.delete(0, tk.END)
            self.dest_entry.insert(0, folder)
    
    def browse_ffmpeg(self):
        file_path = filedialog.askopenfilename(
            title=t('ffmpeg_path'),
            filetypes=[("FFmpeg", "ffmpeg.exe"), ("All files", "*.*")]
        )
        if file_path:
            self.ffmpeg_path.set(file_path)
    
    def browse_virtualdub(self):
        file_path = filedialog.askopenfilename(
            title=t('virtualdub_path'),
            filetypes=[("VirtualDub2", "vdub64.exe"), ("All files", "*.*")]
        )
        if file_path:
            self.virtualdub_path.set(file_path)
    
    def browse_abav1(self):
        file_path = filedialog.askopenfilename(
            title=t('abav1_path'),
            filetypes=[("ab-av1", "ab-av1.exe"), ("All files", "*.*")]
        )
        if file_path:
            self.abav1_path.set(file_path)
    
    def _init_database(self):
        """SQLite adatbázis inicializálása és táblák létrehozása"""
        # Lock használata - biztosítja, hogy egyszerre csak egy adatbázis művelet fusson
        with self.db_lock:
            conn = None
            try:
                # Retry logika SQLITE_BUSY hibákra
                max_retries = 3
                retry_delay = 0.1  # 100ms
                for attempt in range(max_retries):
                    try:
                        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
                        break  # Sikeres kapcsolat
                    except sqlite3.OperationalError as e:
                        if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                            if LOAD_DEBUG:
                                load_debug_log(f"[_init_database] Adatbázis lockolt, újrapróbálás {attempt + 1}/{max_retries}...")
                            time.sleep(retry_delay * (attempt + 1))  # Exponenciális backoff
                            continue
                        else:
                            raise  # Egyéb hiba vagy utolsó próbálkozás
                
                cursor = conn.cursor()
                self._ensure_db_tables(cursor)
                
                conn.commit()
            except (sqlite3.Error, OSError, PermissionError) as e:
                if conn:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                if LOAD_DEBUG:
                    load_debug_log(f"[_init_database] Hiba: {e}")
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"✗ SQLite adatbázis inicializálási hiba: {e}\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass

    def _ensure_db_tables(self, cursor):
        """Biztosítja, hogy a szükséges táblák létezzenek az adatbázisban."""
        try:
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            ''')
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS videos (
                video_path TEXT PRIMARY KEY,
                output_path TEXT,
                order_number INTEGER,
                video_name TEXT,
                status TEXT,
                status_code TEXT,
                cq TEXT,
                vmaf TEXT,
                psnr TEXT,
                progress TEXT,
                orig_size TEXT,
                new_size TEXT,
                size_change TEXT,
                completed_date TEXT,
                orig_size_bytes INTEGER,
                new_size_bytes INTEGER,
                source_frame_count INTEGER,
                source_duration_seconds REAL,
                source_fps REAL,
                source_modified_timestamp REAL,
                output_modified_timestamp REAL,
                output_file_size_bytes INTEGER,
                output_encoder_type TEXT
            )
            ''')
        except sqlite3.Error as e:
            if LOAD_DEBUG:
                load_debug_log(f"[_ensure_db_tables] Hiba a táblák létrehozásakor: {e}")
            raise
        try:
            cursor.execute('ALTER TABLE videos ADD COLUMN source_modified_timestamp REAL')
        except sqlite3.OperationalError as alter_error:
            if 'duplicate column name' not in str(alter_error).lower():
                raise

    def _register_db_thread(self, thread):
        """Nyilvántartjuk az aktív DB mentési szálakat"""
        with self.db_thread_lock:
            self.active_db_threads.append(thread)

    def _unregister_db_thread(self, thread=None):
        """Eltávolítjuk a lezárt DB mentési szálat a nyilvántartásból"""
        current = thread or threading.current_thread()
        with self.db_thread_lock:
            self.active_db_threads = [t for t in self.active_db_threads if t is not current]

    def _start_db_thread(self, target, name=None):
        """Indít egy nem-daemon DB szálat, amely automatikusan lejelentkezik"""
        def wrapper():
            try:
                target()
            finally:
                self._unregister_db_thread()
        thread = threading.Thread(target=wrapper, name=name, daemon=False)
        self._register_db_thread(thread)
        thread.start()
        return thread

    def _wait_for_db_threads(self, timeout=None):
        """Megvárja az összes aktív DB szál befejeződését"""
        end_time = (time.time() + timeout) if timeout else None
        while True:
            with self.db_thread_lock:
                alive_threads = [t for t in self.active_db_threads if t.is_alive()]
                self.active_db_threads = alive_threads
            if not alive_threads:
                break
            for thread in alive_threads:
                remaining = None
                if end_time is not None:
                    remaining = max(0, end_time - time.time())
                try:
                    thread.join(remaining)
                except (RuntimeError, AssertionError):
                    continue
                if end_time is not None and time.time() >= end_time:
                    return
    
    def save_state_to_db(self, progress_callback=None):
        """Táblázat állapot mentése SQLite adatbázisba"""
        # Lock használata - biztosítja, hogy egyszerre csak egy adatbázis művelet fusson
        with self.db_lock:
            conn = None
            try:
                if LOAD_DEBUG:
                    load_debug_log(f"[save_state_to_db] Kezdés | db_path={self.db_path}")
                
                if progress_callback:
                    progress_callback("Adatbázis mentése...")
                
                # Retry logika SQLITE_BUSY hibákra
                max_retries = 3
                retry_delay = 0.1  # 100ms
                for attempt in range(max_retries):
                    try:
                        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
                        break  # Sikeres kapcsolat
                    except sqlite3.OperationalError as e:
                        if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                            if LOAD_DEBUG:
                                load_debug_log(f"[save_state_to_db] Adatbázis lockolt, újrapróbálás {attempt + 1}/{max_retries}...")
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"[save_state_to_db] Adatbázis lockolt, újrapróbálás {attempt + 1}/{max_retries}...\n")
                                    LOG_WRITER.flush()
                                except Exception:
                                    pass
                            time.sleep(retry_delay * (attempt + 1))  # Exponenciális backoff
                            continue
                        else:
                            # Egyéb hiba vagy utolsó próbálkozás - logoljuk
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"✗ [save_state_to_db] SQLite kapcsolódási hiba: {e}\n")
                                    import traceback
                                    LOG_WRITER.write(traceback.format_exc())
                                    LOG_WRITER.flush()
                                except Exception:
                                    pass
                            raise
                
                cursor = conn.cursor()
                self._ensure_db_tables(cursor)
                
                # PRAGMA beállítások - FONTOS: tranzakció előtt kell beállítani!
                # WAL mód beállítása (ha még nincs beállítva)
                try:
                    cursor.execute('PRAGMA journal_mode')
                    current_mode = cursor.fetchone()[0].upper()
                    if current_mode != 'WAL':
                        cursor.execute('PRAGMA journal_mode = WAL')
                except Exception:
                    pass  # Ha hiba van, folytatjuk
                
                # Synchronous beállítása (tranzakció előtt!)
                try:
                    cursor.execute('PRAGMA synchronous = NORMAL')
                except Exception:
                    pass  # Ha hiba van, folytatjuk
                
                # Korábbi értékek betöltése az adatbázisból (ha vannak) - megőrzéshez
                cursor.execute('SELECT video_path, output_modified_timestamp, source_frame_count, source_duration_seconds, source_fps, orig_size_bytes, source_modified_timestamp, output_encoder_type, new_size_bytes FROM videos')
                existing_data = {}
                for row in cursor.fetchall():
                    existing_data[row[0]] = {
                        'output_modified_timestamp': row[1],
                        'source_frame_count': row[2],
                        'source_duration_seconds': row[3],
                        'source_fps': row[4],
                        'orig_size_bytes': row[5],
                        'source_modified_timestamp': row[6],
                        'output_encoder_type': row[7],
                        'new_size_bytes': row[8]  # Output fájl mérete
                    }
                
                # Settings mentése
                # FONTOS: source_path és dest_path mindig el legyen mentve, ha be van állítva
                source_path_str = str(self.source_path) if (hasattr(self, 'source_path') and self.source_path) else None
                dest_path_str = str(self.dest_path) if (hasattr(self, 'dest_path') and self.dest_path) else None
                
                if LOAD_DEBUG:
                    load_debug_log(f"[save_state_to_db] Settings mentése: source_path={source_path_str}, dest_path={dest_path_str}")
                
                settings_data = {
                    'source_path': source_path_str,
                    'dest_path': dest_path_str,
                    'min_vmaf': float(self.min_vmaf.get()),
                    'vmaf_step': float(self.vmaf_step.get()),
                    'max_encoded_percent': int(self.max_encoded_percent.get()),
                    'resize_enabled': bool(self.resize_enabled.get()),
                    'resize_height': int(self.resize_height.get()),
                    'audio_compression_enabled': bool(self.audio_compression_enabled.get()),
                    'audio_compression_method': str(self.audio_compression_method.get()),
                    'auto_vmaf_psnr': bool(self.auto_vmaf_psnr.get()),
                    'svt_preset': int(self.svt_preset.get()),
                    'nvenc_worker_count': int(self.nvenc_worker_count.get())
                }
                
                # Settings tábla frissítése (INSERT OR REPLACE) - batch optimalizáció
                settings_values = [(key, str(value) if value is not None else None) for key, value in settings_data.items()]
                cursor.executemany('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', settings_values)
                
                if LOAD_DEBUG:
                    load_debug_log(f"[save_state_to_db] Settings elmentve: {len(settings_values)} beállítás")
                    load_debug_log(f"[save_state_to_db] Videók számának gyűjtése | video_items={len(self.video_items)}")
                
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"[save_state_to_db] Settings elmentve: {len(settings_values)} beállítás\n")
                        LOG_WRITER.write(f"[save_state_to_db] Videók számának gyűjtése | video_items={len(self.video_items)}\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
                
                # ================================================================================
                # BATCH DIRECTORY SCAN - 10-100× GYORSABB mint egyesével stat()!
                # ================================================================================
                # Egyszer végigmegy az összes fájlon (mint "dir /s"), cache-eli,
                # aztán csak összehasonlít a DB-vel. Csak a változott fájlokat probe-olja.
                
                if progress_callback:
                    progress_callback("Fájlok szkennelése...")
                
                scan_start_time = time.time()
                
                # Source könyvtár scan
                source_scan = {}
                if self.source_path and self.source_path.exists():
                    try:
                        source_scan = batch_scan_directory(self.source_path)
                        if LOAD_DEBUG:
                            load_debug_log(f"[save_state_to_db] Source scan: {len(source_scan)} fájl | {time.time() - scan_start_time:.2f}s")
                    except Exception as e:
                        if LOAD_DEBUG:
                            load_debug_log(f"[save_state_to_db] Source scan hiba: {e}")
                        if LOG_WRITER:
                            try:
                                LOG_WRITER.write(f"⚠ [save_state_to_db] Source scan hiba: {e}\n")
                                LOG_WRITER.flush()
                            except Exception:
                                pass
                
                # Dest könyvtár scan
                dest_scan = {}
                if self.dest_path and self.dest_path.exists():
                    try:
                        dest_scan = batch_scan_directory(self.dest_path)
                        if LOAD_DEBUG:
                            load_debug_log(f"[save_state_to_db] Dest scan: {len(dest_scan)} fájl | {time.time() - scan_start_time:.2f}s")
                    except Exception as e:
                        if LOAD_DEBUG:
                            load_debug_log(f"[save_state_to_db] Dest scan hiba: {e}")
                        if LOG_WRITER:
                            try:
                                LOG_WRITER.write(f"⚠ [save_state_to_db] Dest scan hiba: {e}\n")
                                LOG_WRITER.flush()
                            except Exception:
                                pass
                
                scan_time = time.time() - scan_start_time
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"[save_state_to_db] Batch scan befejezve: {len(source_scan)} source + {len(dest_scan)} dest fájl | {scan_time:.2f}s\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
                
                if progress_callback:
                    progress_callback(f"Szkennelés kész ({len(source_scan)} + {len(dest_scan)} fájl)")
                
                # ================================================================================
                
                processed_count = 0
                total_videos = len(self.video_items)
                last_log_time = time.time()
                start_time = time.time()
                
                # Batch optimalizáció: előkészítjük az összes videó adatát, majd batch insert
                videos_data = []
                
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"[save_state_to_db] Videók előkészítése kezdődik | összesen: {total_videos} videó\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
                
                if progress_callback:
                    try:
                        progress_callback(f"Videók előkészítése... (0/{total_videos})")
                    except Exception:
                        pass
                
                # OPTIMALIZÁCIÓ: Először összegyűjtjük az összes tree adatot egyszerre (gyorsabb, mint egyesével)
                # Ez jelentősen gyorsítja a folyamatot, mert a tree.item() hívás lassú lehet
                tree_data_cache = {}
                tree_collect_start = time.time()
                for video_path, item_id in self.video_items.items():
                    try:
                        tree_data_cache[video_path] = self.tree.item(item_id)['values']
                    except (tk.TclError, KeyError, AttributeError):
                        tree_data_cache[video_path] = []
                
                tree_collect_time = time.time() - tree_collect_start
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"[save_state_to_db] Tree adatok összegyűjtve: {len(tree_data_cache)} videó | időtartam: {tree_collect_time:.2f}s\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
                
                # Optimalizáció: először gyűjtsük össze az összes stat() hívást, hogy ne legyen szétszórt
                # De mivel a probolás csak szükség esetén történik, ezt nem lehet teljesen optimalizálni
                
                for video_path, item_id in self.video_items.items():
                    try:
                        values = tree_data_cache.get(video_path, [])
                        output_file = self.video_to_output.get(video_path)
                        order_num = self.video_order.get(video_path, 0)
                        
                        # Probe eredmények lekérdezése - korábbi adatbázisból, ha van (gyors)
                        existing_video_data = existing_data.get(str(video_path), {})
                        
                        # OPTIMALIZÁCIÓ: Hidegindításnál (nincs existing_video_data) minimalizáljuk a string műveleteket
                        # Csak akkor normalizálunk, ha tényleg szükséges (melegindításnál vagy ha változott)
                        is_cold_start = not existing_video_data
                        
                        # OPTIMALIZÁCIÓ: Hidegindításnál egyszer lekérdezzük a tree_item_data-t, és többször használjuk
                        original_data = None
                        if is_cold_start:
                            original_data = self.tree_item_data.get(item_id, {})
                        
                        if is_cold_start:
                            # Hidegindításnál: először próbáljuk a tree_item_data-t (gyors, parse-olás nélkül!)
                            
                            # CQ, VMAF, PSNR: tree_item_data-ból, ha van
                            if original_data and 'cq' in original_data:
                                cq_val = str(original_data['cq'])
                            else:
                                cq_val = values[self.COLUMN_INDEX['cq']] if len(values) > self.COLUMN_INDEX['cq'] else "-"
                            
                            if original_data and 'vmaf' in original_data:
                                vmaf_val = str(original_data['vmaf'])
                            else:
                                vmaf_val = values[self.COLUMN_INDEX['vmaf']] if len(values) > self.COLUMN_INDEX['vmaf'] else "-"
                            
                            if original_data and 'psnr' in original_data:
                                psnr_val = str(original_data['psnr'])
                            else:
                                psnr_val = values[self.COLUMN_INDEX['psnr']] if len(values) > self.COLUMN_INDEX['psnr'] else "-"
                            
                            # Méretek: tree-ből (MB formátum), de new_size_bytes tree_item_data-ból, ha van
                            orig_size_normalized = values[self.COLUMN_INDEX['orig_size']] if len(values) > self.COLUMN_INDEX['orig_size'] else "-"
                            new_size_normalized = values[self.COLUMN_INDEX['new_size']] if len(values) > self.COLUMN_INDEX['new_size'] else "-"
                            size_change_normalized = values[self.COLUMN_INDEX['size_change']] if len(values) > self.COLUMN_INDEX['size_change'] else "-"
                        else:
                            # Melegindításnál: normalizáljuk a számokat (lehet, hogy változott a nyelv vagy formátum)
                            cq_val = values[self.COLUMN_INDEX['cq']] if len(values) > self.COLUMN_INDEX['cq'] else "-"
                            vmaf_val = normalize_number_string(values[self.COLUMN_INDEX['vmaf']]) if len(values) > self.COLUMN_INDEX['vmaf'] else "-"
                            psnr_val = normalize_number_string(values[self.COLUMN_INDEX['psnr']]) if len(values) > self.COLUMN_INDEX['psnr'] else "-"
                            orig_size_val = values[self.COLUMN_INDEX['orig_size']] if len(values) > self.COLUMN_INDEX['orig_size'] else "-"
                            new_size_val = values[self.COLUMN_INDEX['new_size']] if len(values) > self.COLUMN_INDEX['new_size'] else "-"
                            size_change_val = values[self.COLUMN_INDEX['size_change']] if len(values) > self.COLUMN_INDEX['size_change'] else "-"
                            
                            # Méretek normalizálása (MB és % jelek megtartása, de számok nyelvfüggetlen formátumban)
                            if orig_size_val != "-" and "MB" in orig_size_val:
                                orig_size_normalized = normalize_number_string(orig_size_val.replace("MB", "").strip()) + " MB"
                            else:
                                orig_size_normalized = orig_size_val
                            
                            if new_size_val != "-" and "MB" in new_size_val:
                                new_size_normalized = normalize_number_string(new_size_val.replace("MB", "").strip()) + " MB"
                            else:
                                new_size_normalized = new_size_val
                            
                            if size_change_val != "-" and "%" in size_change_val:
                                # Megtartjuk a + jelet, ha van
                                has_plus = size_change_val.strip().startswith('+')
                                clean_val = size_change_val.replace("%", "").replace("+", "").strip()
                                normalized_num = normalize_number_string(clean_val)
                                size_change_normalized = ("+" if has_plus else "") + normalized_num + "%"
                            else:
                                size_change_normalized = size_change_val
                        
                        # Melegindításnál: DB-ből olvassuk az értékeket
                        # Hidegindításnál: tree_item_data-ból vagy parse-olásból (lásd lent)
                        if not is_cold_start:
                            source_frame_count = existing_video_data.get('source_frame_count')
                            source_duration_seconds = existing_video_data.get('source_duration_seconds')
                            source_fps = existing_video_data.get('source_fps')
                        else:
                            # Hidegindításnál: inicializáljuk None-re, majd tree_item_data-ból vagy parse-olásból töltjük fel
                            source_frame_count = None
                            source_duration_seconds = None
                            source_fps = None
                        
                        # Output fájl metaadatainak mentése
                        output_modified_timestamp = None
                        # Megjegyzés: output_file_size_bytes = new_size_bytes_val (lásd 7799. sor)
                        output_encoder_type = None
                        
                        status_code = normalize_status_to_code(values[self.COLUMN_INDEX['status']] if len(values) > self.COLUMN_INDEX['status'] else "")
                        
                        # Source videó stat() és probolás - optimalizálva
                        # Hidegindításnál (nincs DB bejegyzés) probolunk és stat()-olunk
                        # Start gomb után (van DB bejegyzés) csak akkor probolunk/stat()-olunk, ha változott a fájl
                        source_stat_info = None
                        needs_source_probe = False
                        
                        if is_cold_start:
                            # Hidegindításnál: OPTIMALIZÁLT - tree item mögötti eredeti adatok használata
                            # Először próbáljuk a tree item mögötti eredeti adatokat (gyors, nincs parse-olás!)
                            # Megjegyzés: original_data már lekérdezve van fent (optimalizáció)
                            if original_data:
                                source_duration_seconds = original_data.get('source_duration_seconds')
                                source_frame_count = original_data.get('source_frame_count')
                                source_fps = original_data.get('source_fps')
                            else:
                                source_duration_seconds = None
                                source_frame_count = None
                                source_fps = None
                            
                            # Ha nincs tree item mögötti adat, fallback: parse-olás a tree-ből
                            if source_duration_seconds is None or source_frame_count is None:
                                duration_str = values[self.COLUMN_INDEX['duration']] if len(values) > self.COLUMN_INDEX['duration'] else "-"
                                frames_str = values[self.COLUMN_INDEX['frames']] if len(values) > self.COLUMN_INDEX['frames'] else "-"
                                
                                if duration_str != "-" and frames_str != "-":
                                    try:
                                        # Gyors parse-olás - minimalizáljuk a string műveleteket
                                        if "s" in duration_str.lower():
                                            source_duration_seconds = float(duration_str.lower().replace("s", "").strip().replace(",", "."))
                                        else:
                                            source_duration_seconds = float(duration_str.replace(",", "."))
                                        
                                        frames_clean = frames_str.replace(",", "").replace(" ", "")
                                        source_frame_count = int(frames_clean) if frames_clean.isdigit() else None
                                        
                                        # FPS számítás
                                        if source_duration_seconds and source_frame_count:
                                            source_fps = source_frame_count / source_duration_seconds
                                    except (ValueError, TypeError, AttributeError):
                                        pass
                            
                            # Cache-elt stat() értékek használata
                            cached_stat = self.video_stat_cache.get(video_path)
                            if cached_stat and cached_stat.get('source_size_bytes') is not None:
                                orig_size_bytes_val = cached_stat['source_size_bytes']
                                source_modified_timestamp = cached_stat.get('source_modified_timestamp')
                            else:
                                orig_size_bytes_val = None
                                source_modified_timestamp = None
                            
                            source_stat_info = None
                            needs_source_probe = False
                            
                            # Fallback: ha még mindig nincs érték, próbáljuk a tree-ből parse-olni (utolsó esély)
                            if orig_size_bytes_val is None:
                                orig_size_bytes_val = parse_size_to_bytes(values[self.COLUMN_INDEX['orig_size']] if len(values) > self.COLUMN_INDEX['orig_size'] else "-")
                        else:
                            # Melegindításnál: inicializáljuk az értékeket
                            source_frame_count = existing_video_data.get('source_frame_count')
                            source_duration_seconds = existing_video_data.get('source_duration_seconds')
                            source_fps = existing_video_data.get('source_fps')
                            
                            # OPTIMALIZÁLT: Használjuk a batch scan eredményt! (nem stat()!)
                            # Van DB bejegyzés - csak akkor probolunk, ha változott a fájl
                            scan_info = source_scan.get(video_path)
                            if scan_info:
                                # Fájl megvan a scan-ben - összehasonlítás DB értékekkel
                                saved_source_size = existing_video_data.get('orig_size_bytes')
                                saved_source_timestamp = existing_video_data.get('source_modified_timestamp')
                                
                                # Csak akkor probolunk, ha TÉNYLEGESEN változott a fájl (méret VAGY dátum)
                                if saved_source_size is not None and scan_info['size'] != saved_source_size:
                                    needs_source_probe = True
                                elif saved_source_timestamp is not None and abs(scan_info['mtime'] - saved_source_timestamp) > 1.0:
                                    needs_source_probe = True
                                
                                # Update size/mtime from scan (no need to stat again!)
                                orig_size_bytes_val = scan_info['size']
                                source_modified_timestamp = scan_info['mtime']
                                source_stat_info = None  # Not needed
                            else:
                                # Fájl nincs a scan-ben (törölve vagy nem elérhető)
                                # Használjuk a DB-ből az értékeket
                                orig_size_bytes_val = existing_video_data.get('orig_size_bytes')
                                source_modified_timestamp = existing_video_data.get('source_modified_timestamp')
                                source_stat_info = None
                        
                        # Source videó probolás - minden lényeges infót beolvasunk
                        if needs_source_probe:
                            try:
                                if LOAD_DEBUG:
                                    if existing_video_data:
                                        load_debug_log(f"[save_state_to_db] Source videó probolás (fájl változott): {video_path}")
                                    else:
                                        load_debug_log(f"[save_state_to_db] Source videó probolás (hidegindítás/új videó): {video_path}")
                                # Frame count, duration, fps - egyetlen probolással
                                source_frame_count = get_video_frame_count(video_path)
                                source_duration_seconds, source_fps = get_video_info(video_path)
                            except Exception as e:
                                if LOAD_DEBUG:
                                    load_debug_log(f"[save_state_to_db] Source videó probolás hiba: {e}")
                        
                        # Output file stat() és probolás - optimalizálva
                        # Hidegindításnál (nincs DB bejegyzés) probolunk és stat()-olunk
                        # Start gomb után (van DB bejegyzés) csak akkor probolunk/stat()-olunk, ha változott a fájl
                        output_stat_info = None
                        needs_output_probe = False
                        
                        if status_code in ('completed', 'completed_nvenc', 'completed_svt', 'completed_copy', 'completed_exists'):
                            # OPTIMALIZÁCIÓ: Hidegindításnál ne ellenőrizzük az output fájlokat, mert a betöltéskor már ellenőriztük!
                            # Csak akkor ellenőrizzük, ha melegindításnál van DB bejegyzés (lehet, hogy változott a fájl)
                            if output_file:
                                if not existing_video_data:
                                    # Hidegindításnál: a betöltéskor már ellenőriztük az output fájlokat
                                    # Ne hívjuk meg újra az exists() és stat() hívásokat!
                                    # A tree-ből olvassuk az adatokat, ha van
                                    # Csak akkor probolunk, ha a tree-ben nincs encoder_type adat
                                    needs_output_probe = False
                                    output_stat_info = None
                                    # Próbáljuk a tree-ből vagy cache-ből beolvasni az adatokat
                                    # (ha van output fájl, akkor valószínűleg van adat a tree-ben vagy cache-ben)
                                else:
                                    # OPTIMALIZÁLT: Használjuk a batch scan eredményt! (nem stat()!)
                                    # Melegindításnál: csak akkor stat()-olunk/probolunk, ha változott a fájl
                                    output_scan_info = dest_scan.get(output_file)
                                    if output_scan_info:
                                        # Output fájl megvan a scan-ben - összehasonlítás DB értékekkel
                                        saved_output_timestamp = existing_video_data.get('output_modified_timestamp')
                                        saved_output_size = existing_video_data.get('new_size_bytes')
                                        
                                        # Csak akkor probolunk, ha TÉNYLEGESEN változott a fájl (méret VAGY dátum)
                                        if saved_output_size is not None and output_scan_info['size'] != saved_output_size:
                                            needs_output_probe = True
                                        elif saved_output_timestamp is not None and abs(output_scan_info['mtime'] - saved_output_timestamp) > 1.0:
                                            needs_output_probe = True
                                        
                                        # Update size/mtime from scan (no need to stat again!)
                                        output_modified_timestamp = output_scan_info['mtime']
                                        # new_size_bytes frissítése később (output fájl méret)
                                        output_stat_info = None  # Not needed
                                    else:
                                        # Output fájl nincs a scan-ben (törölve vagy hiányzik)
                                        output_stat_info = None
                                        needs_output_probe = False
                                
                                # Output fájl probolás - minden lényeges infót beolvasunk (encoder_type)
                                if needs_output_probe:
                                    try:
                                        if LOAD_DEBUG:
                                            if existing_video_data:
                                                load_debug_log(f"[save_state_to_db] Output videó probolás (fájl változott): {output_file}")
                                            else:
                                                load_debug_log(f"[save_state_to_db] Output videó probolás (hidegindítás/új videó): {output_file}")
                                        # Teljes probe - Settings tag-ből encoder_type
                                        probe_cmd = [
                                            FFPROBE_PATH, '-v', 'error',
                                            '-show_entries', 'format_tags=Settings',
                                            '-of', 'default=noprint_wrappers=1:nokey=1',
                                            os.fspath(output_file.absolute())
                                        ]
                                        result_probe = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=5, startupinfo=get_startup_info())
                                        settings_str = result_probe.stdout.strip() if result_probe.stdout else ""
                                        if settings_str:
                                            if 'NVENC' in settings_str.upper() or 'CQ:' in settings_str:
                                                output_encoder_type = 'nvenc'
                                            elif 'SVT-AV1' in settings_str.upper() or 'SVT' in settings_str.upper() or 'CRF:' in settings_str:
                                                output_encoder_type = 'svt-av1'
                                    except Exception as e:
                                        if LOAD_DEBUG:
                                            load_debug_log(f"[save_state_to_db] Output videó probolás hiba: {e}")
                                
                                # Hidegindításnál: próbáljuk a tree item mögötti eredeti adatokat (gyors!)
                                # Megjegyzés: original_data már lekérdezve van fent (optimalizáció)
                                if is_cold_start and not output_encoder_type and original_data:
                                    output_encoder_type = original_data.get('output_encoder_type')
                                
                                # Használjuk a mentett értéket, ha van (Start gomb után, ha nem proboltunk)
                                if not output_encoder_type and existing_video_data:
                                    output_encoder_type = existing_video_data.get('output_encoder_type')
                        
                        # Source modified timestamp és orig_size_bytes_val - optimalizálva
                        # Hidegindításnál: már beállítottuk a cache-ből (lásd fent)
                        # Melegindításnál: DB-ből vagy stat()-ból, attól függően, hogy változott-e
                        if not is_cold_start:
                            # Melegindításnál: DB-ből vagy stat()-ból
                            source_modified_timestamp = existing_video_data.get('source_modified_timestamp')
                            orig_size_bytes_val = existing_video_data.get('orig_size_bytes')
                            # Melegindításnál: ha stat()-oltunk, frissítjük mindkét értéket
                            if source_stat_info:
                                # Frissítjük a stat() eredményből (még akkor is, ha nem proboltunk)
                                orig_size_bytes_val = source_stat_info.st_size
                                source_modified_timestamp = source_stat_info.st_mtime
                            elif orig_size_bytes_val is None and video_path.exists():
                                # Ha nincs mentett érték és nincs korábbi stat(), akkor stat()-olunk
                                try:
                                    stat_info = video_path.stat()
                                    orig_size_bytes_val = stat_info.st_size
                                    if source_modified_timestamp is None:
                                        source_modified_timestamp = stat_info.st_mtime
                                except (OSError, PermissionError):
                                    pass
                        
                        # Ha még mindig nincs érték, próbáljuk a táblázatból parse-olni (utolsó esély)
                        if orig_size_bytes_val is None:
                            orig_size_bytes_val = parse_size_to_bytes(values[self.COLUMN_INDEX['orig_size']] if len(values) > self.COLUMN_INDEX['orig_size'] else "-")
                        
                        # Frissítsük az orig_size_str-t is, ha "-" volt
                        if orig_size_normalized == "-" and orig_size_bytes_val is not None:
                            orig_size_mb = orig_size_bytes_val / (1024**2)
                            orig_size_normalized = f"{format_localized_number(orig_size_mb, decimals=1)} MB"
                        
                        # Output file size - először tree_item_data-ból (gyors, parse-olás nélkül!), majd fallback tree-ből
                        # Megjegyzés: original_data már lekérdezve van fent (optimalizáció)
                        if is_cold_start and original_data and 'new_size_bytes' in original_data:
                            new_size_bytes_val = original_data['new_size_bytes']
                        else:
                            new_size_bytes_val = parse_size_to_bytes(values[self.COLUMN_INDEX['new_size']] if len(values) > self.COLUMN_INDEX['new_size'] else "-")
                        
                        # Output modified timestamp és file size - optimalizálva
                        # Először próbáljuk az adatbázisból (gyors), de ha stat()-oltunk, frissítjük a stat() eredményből
                        output_modified_timestamp = existing_video_data.get('output_modified_timestamp')
                        
                        if output_stat_info:
                            # Frissítjük a stat() eredményből (még akkor is, ha nem proboltunk)
                            output_modified_timestamp = output_stat_info.st_mtime
                            # A new_size_bytes_val a táblázatból jön, de ellenőrizzük, hogy egyezik-e a fájlmérettel
                            actual_output_size = output_stat_info.st_size
                            # Ha a táblázatból jövő érték eltér a tényleges fájlmérettől, használjuk a ténylegeset
                            if actual_output_size is not None and (new_size_bytes_val is None or abs(new_size_bytes_val - actual_output_size) > 1024):  # 1KB tolerancia
                                new_size_bytes_val = actual_output_size
                        elif output_modified_timestamp is None and output_file and output_file.exists() and status_code in ('completed', 'completed_nvenc', 'completed_svt', 'completed_copy', 'completed_exists'):
                            # Ha nincs mentett timestamp, de completed státusz van, stat()-olunk
                            try:
                                stat_info = output_file.stat()
                                output_modified_timestamp = stat_info.st_mtime
                                actual_output_size = stat_info.st_size
                                if actual_output_size is not None and (new_size_bytes_val is None or abs(new_size_bytes_val - actual_output_size) > 1024):
                                    new_size_bytes_val = actual_output_size
                            except (OSError, PermissionError):
                                pass
                        
                        # Adatok hozzáadása a batch listához
                        videos_data.append((
                            str(video_path),
                            str(output_file) if output_file else None,
                            int(order_num),
                            str(values[self.COLUMN_INDEX['video_name']]) if len(values) > self.COLUMN_INDEX['video_name'] else "",
                            str(values[self.COLUMN_INDEX['status']]) if len(values) > self.COLUMN_INDEX['status'] else "",
                            status_code,
                            str(cq_val),
                            vmaf_val,
                            psnr_val,
                            str(values[self.COLUMN_INDEX['progress']]) if len(values) > self.COLUMN_INDEX['progress'] else "-",
                            orig_size_normalized,
                            new_size_normalized,
                            size_change_normalized,
                            str(values[self.COLUMN_INDEX['completed_date']]) if len(values) > self.COLUMN_INDEX['completed_date'] else "",
                            orig_size_bytes_val,
                            new_size_bytes_val,
                            source_frame_count,
                            source_duration_seconds,
                            source_fps,
                            source_modified_timestamp,
                            output_modified_timestamp,
                            new_size_bytes_val,  # output_file_size_bytes = new_size_bytes (optimalizálva)
                            output_encoder_type
                        ))
                        
                        # Progress logolás - gyakrabban, hogy lássuk a haladást
                        processed_count += 1
                        current_time = time.time()
                        elapsed_time = current_time - start_time
                        if processed_count % 50 == 0 or (current_time - last_log_time) >= 2.0:
                            speed = processed_count / elapsed_time if elapsed_time > 0 else 0
                            if LOAD_DEBUG:
                                load_debug_log(f"[save_state_to_db] Folyamatban: {processed_count}/{total_videos} videó előkészítve | sebesség: {speed:.1f} videó/s | eltelt idő: {elapsed_time:.1f}s")
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"[save_state_to_db] Folyamatban: {processed_count}/{total_videos} videó előkészítve | sebesség: {speed:.1f} videó/s | eltelt idő: {elapsed_time:.1f}s\n")
                                    LOG_WRITER.flush()
                                except Exception:
                                    pass
                            if progress_callback:
                                try:
                                    progress_callback(f"Videók előkészítése... ({processed_count}/{total_videos})")
                                except Exception:
                                    pass
                            last_log_time = current_time
                    
                    except (tk.TclError, KeyError, AttributeError, IndexError) as e:
                        processed_count += 1
                        if LOAD_DEBUG and processed_count % 100 == 0:
                            load_debug_log(f"[save_state_to_db] Videó feldolgozási hiba: {video_path} -> {e}")
                        # Logoljuk a hibát az av1_recompress.log-ba is
                        if LOG_WRITER and processed_count % 100 == 0:
                            try:
                                LOG_WRITER.write(f"⚠ [save_state_to_db] Videó feldolgozási hiba: {video_path} -> {e}\n")
                                LOG_WRITER.flush()
                            except Exception:
                                pass
                        continue
                
                # Előkészítés befejezve - logolás
                prep_time = time.time() - start_time
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"[save_state_to_db] Videók előkészítése befejezve | {processed_count}/{total_videos} videó | időtartam: {prep_time:.2f}s | sebesség: {processed_count/prep_time:.1f} videó/s\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
                if LOAD_DEBUG:
                    load_debug_log(f"[save_state_to_db] Videók előkészítése befejezve | {processed_count}/{total_videos} videó | időtartam: {prep_time:.2f}s")
                if progress_callback:
                    try:
                        progress_callback(f"Adatbázis írása... ({len(videos_data)} videó)")
                    except Exception:
                        pass
                
                # Batch INSERT - sokkal gyorsabb, mint egyesével
                # Megjegyzés: PRAGMA beállítások már a kapcsolat létrehozása után, tranzakció előtt történtek
                
                if videos_data:
                    if LOAD_DEBUG:
                        load_debug_log(f"[save_state_to_db] Batch INSERT kezdése: {len(videos_data)} videó")
                    if progress_callback:
                        progress_callback(f"Adatbázis írása... ({len(videos_data)} videó)")
                    
                    # Nagy batch-eket kisebbekre bontjuk, hogy ne legyen túl nagy a journal fájl
                    batch_size = 1000  # 1000 videó per batch
                    total_batches = (len(videos_data) + batch_size - 1) // batch_size
                    
                    for batch_idx in range(total_batches):
                        start_idx = batch_idx * batch_size
                        end_idx = min(start_idx + batch_size, len(videos_data))
                        batch_data = videos_data[start_idx:end_idx]
                        
                        if progress_callback and total_batches > 1:
                            progress_callback(f"Adatbázis írása... ({end_idx}/{len(videos_data)})")
                        
                        try:
                            cursor.executemany('''
                                INSERT OR REPLACE INTO videos (
                                    video_path, output_path, order_number, video_name, status, status_code,
                                    cq, vmaf, psnr, progress, orig_size, new_size, size_change, completed_date,
                                    orig_size_bytes, new_size_bytes, source_frame_count, source_duration_seconds, source_fps,
                                    source_modified_timestamp, output_modified_timestamp, output_file_size_bytes, output_encoder_type
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', batch_data)
                        except (sqlite3.Error, sqlite3.OperationalError, sqlite3.IntegrityError) as db_error:
                            # DB INSERT hiba - logoljuk az av1_recompress.log-ba
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"✗ [save_state_to_db] SQLite INSERT hiba (batch {batch_idx + 1}/{total_batches}): {db_error}\n")
                                    import traceback
                                    LOG_WRITER.write(traceback.format_exc())
                                    LOG_WRITER.flush()
                                except Exception:
                                    pass
                            raise  # Dobjuk tovább a hibát
                        
                        # Commit minden batch után, hogy ne legyen túl nagy a journal fájl
                        try:
                            conn.commit()
                        except (sqlite3.Error, sqlite3.OperationalError) as commit_error:
                            # Commit hiba - logoljuk az av1_recompress.log-ba
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"✗ [save_state_to_db] SQLite COMMIT hiba (batch {batch_idx + 1}/{total_batches}): {commit_error}\n")
                                    import traceback
                                    LOG_WRITER.write(traceback.format_exc())
                                    LOG_WRITER.flush()
                                except Exception:
                                    pass
                            raise  # Dobjuk tovább a hibát
                    
                    # Végleges WAL checkpoint - ez törli a journal fájlt és biztosítja, hogy a változások a fő adatbázisba kerüljenek
                    try:
                        cursor.execute('PRAGMA wal_checkpoint(TRUNCATE)')
                        if LOAD_DEBUG:
                            load_debug_log(f"[save_state_to_db] WAL checkpoint végrehajtva")
                    except Exception as checkpoint_error:
                        if LOAD_DEBUG:
                            load_debug_log(f"[save_state_to_db] WAL checkpoint hiba: {checkpoint_error}")
                        # Logoljuk a hibát az av1_recompress.log-ba
                        if LOG_WRITER:
                            try:
                                LOG_WRITER.write(f"⚠ [save_state_to_db] WAL checkpoint hiba: {checkpoint_error}\n")
                                import traceback
                                LOG_WRITER.write(traceback.format_exc())
                                LOG_WRITER.flush()
                            except Exception:
                                pass
                        # Ha a checkpoint nem sikerül, próbáljuk meg újra
                        try:
                            conn.commit()  # Biztosítjuk, hogy a változások commitolva legyenek
                            cursor.execute('PRAGMA wal_checkpoint(TRUNCATE)')
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"✓ [save_state_to_db] WAL checkpoint újrapróbálás sikeres\n")
                                    LOG_WRITER.flush()
                                except Exception:
                                    pass
                        except Exception as retry_error:
                            # Retry is sikertelen - logoljuk
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"✗ [save_state_to_db] WAL checkpoint újrapróbálás is sikertelen: {retry_error}\n")
                                    import traceback
                                    LOG_WRITER.write(traceback.format_exc())
                                    LOG_WRITER.flush()
                                except Exception:
                                    pass
                
                # Ellenőrizzük, hogy tényleg mentődtek-e az adatok
                if videos_data:
                    try:
                        cursor.execute('SELECT COUNT(*) FROM videos')
                        saved_count = cursor.fetchone()[0]
                        if LOAD_DEBUG:
                            load_debug_log(f"[save_state_to_db] DB-ben mentett videók száma: {saved_count}")
                        if saved_count == 0 and len(videos_data) > 0:
                            error_msg = f"⚠ FIGYELEM: {len(videos_data)} videó előkészítve, de 0 mentve a DB-be!"
                            if LOAD_DEBUG:
                                load_debug_log(f"[save_state_to_db] {error_msg}")
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"{error_msg}\n")
                                    LOG_WRITER.flush()
                                except Exception:
                                    pass
                            if progress_callback:
                                try:
                                    progress_callback(error_msg)
                                except Exception:
                                    pass
                    except (sqlite3.Error, sqlite3.OperationalError) as count_error:
                        # COUNT hiba - logoljuk az av1_recompress.log-ba
                        if LOG_WRITER:
                            try:
                                LOG_WRITER.write(f"✗ [save_state_to_db] SQLite COUNT hiba: {count_error}\n")
                                import traceback
                                LOG_WRITER.write(traceback.format_exc())
                                LOG_WRITER.flush()
                            except Exception:
                                pass
                
                if LOAD_DEBUG:
                    load_debug_log(f"[save_state_to_db] Videók feldolgozva: {processed_count}/{total_videos}")
                    load_debug_log(f"✓ SQLite állapot sikeresen mentve: {self.db_path}")
                
                if progress_callback:
                    try:
                        progress_callback(f"✓ Adatbázis mentve ({processed_count} videó)")
                    except Exception:
                        pass
                
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"✓ SQLite állapot sikeresen mentve: {self.db_path} ({processed_count} videó)\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
                
            except Exception as e:
                # SQLite mentés hiba
                if conn:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                error_msg = f"✗ SQLite mentés hiba: {e}"
                if LOAD_DEBUG:
                    load_debug_log(f"[save_state_to_db] HIBA: {error_msg}")
                    import traceback
                    load_debug_log(traceback.format_exc())
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"{error_msg}\n")
                        import traceback
                        LOG_WRITER.write(traceback.format_exc())
                        LOG_WRITER.flush()
                    except Exception:
                        pass
                # Hibaüzenet küldése a GUI-nak is
                if progress_callback:
                    try:
                        progress_callback(f"✗ Adatbázis mentés hiba: {e}")
                    except Exception:
                        pass
                # Dobjuk tovább a hibát, hogy a save_db_async elkapja
                raise
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass
    
    def save_settings_to_db(self):
        """Csak a beállítások mentése az adatbázisba (gyors, nem menti a videó adatokat)"""
        # Lock használata - biztosítja, hogy egyszerre csak egy adatbázis művelet fusson
        with self.db_lock:
            conn = None
            try:
                # Retry logika SQLITE_BUSY hibákra
                max_retries = DB_RETRY_MAX_ATTEMPTS
                retry_delay = DB_RETRY_DELAY
                for attempt in range(max_retries):
                    try:
                        conn = sqlite3.connect(str(self.db_path), timeout=DB_CONNECTION_TIMEOUT)
                        break  # Sikeres kapcsolat
                    except sqlite3.OperationalError as e:
                        if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                            if LOAD_DEBUG:
                                load_debug_log(f"[save_settings_to_db] Adatbázis lockolt, újrapróbálás {attempt + 1}/{max_retries}...")
                            time.sleep(retry_delay * (attempt + 1))  # Exponenciális backoff
                            continue
                        else:
                            raise  # Egyéb hiba vagy utolsó próbálkozás
                
                cursor = conn.cursor()
                self._ensure_db_tables(cursor)
                
                # Settings mentése
                # FONTOS: source_path és dest_path mindig el legyen mentve, ha be van állítva
                source_path_str = str(self.source_path) if (hasattr(self, 'source_path') and self.source_path) else None
                dest_path_str = str(self.dest_path) if (hasattr(self, 'dest_path') and self.dest_path) else None
                
                if LOAD_DEBUG:
                    load_debug_log(f"[save_settings_to_db] Settings mentése: source_path={source_path_str}, dest_path={dest_path_str}")
                
                settings_data = {
                    'source_path': source_path_str,
                    'dest_path': dest_path_str,
                    'min_vmaf': float(self.min_vmaf.get()),
                    'vmaf_step': float(self.vmaf_step.get()),
                    'max_encoded_percent': int(self.max_encoded_percent.get()),
                    'resize_enabled': bool(self.resize_enabled.get()),
                    'resize_height': int(self.resize_height.get()),
                    'audio_compression_enabled': bool(self.audio_compression_enabled.get()),
                    'audio_compression_method': str(self.audio_compression_method.get()),
                    'auto_vmaf_psnr': bool(self.auto_vmaf_psnr.get()),
                    'svt_preset': int(self.svt_preset.get()),
                    'nvenc_worker_count': int(self.nvenc_worker_count.get())
                }
                
                # Settings tábla frissítése (INSERT OR REPLACE) - batch optimalizáció
                settings_values = [(key, str(value) if value is not None else None) for key, value in settings_data.items()]
                cursor.executemany('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', settings_values)
                
                if LOAD_DEBUG:
                    load_debug_log(f"[save_settings_to_db] Settings elmentve: {len(settings_values)} beállítás")
                
                conn.commit()
            except (sqlite3.Error, OSError, PermissionError) as e:
                if conn:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                if LOAD_DEBUG:
                    load_debug_log(f"[save_settings_to_db] Hiba: {e}")
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass
    
    def update_single_video_in_db(self, video_path, item_id, status_text, cq_str, vmaf_str, psnr_str, orig_size_str, new_size_mb, change_percent, completed_date):
        """Egyetlen videó adatbázis-bejegyzésének frissítése (encoding befejezése után)"""
        if not hasattr(self, 'db_path') or not self.db_path:
            return  # Nincs adatbázis
        
        # Lock használata - biztosítja, hogy egyszerre csak egy adatbázis művelet fusson
        with self.db_lock:
            conn = None
            try:
                # Retry logika SQLITE_BUSY hibákra
                max_retries = 3
                retry_delay = 0.1  # 100ms
                for attempt in range(max_retries):
                    try:
                        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
                        break  # Sikeres kapcsolat
                    except sqlite3.OperationalError as e:
                        if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                            time.sleep(retry_delay * (attempt + 1))  # Exponenciális backoff
                            continue
                        else:
                            raise  # Egyéb hiba vagy utolsó próbálkozás
                
                cursor = conn.cursor()
                self._ensure_db_tables(cursor)
                
                # PRAGMA beállítások
                try:
                    cursor.execute('PRAGMA journal_mode = WAL')
                    cursor.execute('PRAGMA synchronous = NORMAL')
                except Exception:
                    pass
                
                # Meglévő adatok lekérdezése (ha vannak)
                video_path_str = str(video_path)
                cursor.execute('SELECT output_path, order_number, video_name, orig_size_bytes, new_size_bytes, source_frame_count, source_duration_seconds, source_fps, source_modified_timestamp, output_modified_timestamp, output_file_size_bytes, output_encoder_type FROM videos WHERE video_path = ?', (video_path_str,))
                existing_row = cursor.fetchone()
                
                # Tree adatok lekérdezése
                try:
                    values = self.tree.item(item_id, 'values')
                except (tk.TclError, KeyError, AttributeError):
                    values = []
                
                # Output fájl meghatározása
                output_file = self.video_to_output.get(video_path)
                if not output_file:
                    output_file = get_output_filename(video_path, self.source_path, self.dest_path)
                
                output_path_str = str(output_file) if output_file else None
                order_num = self.video_order.get(video_path, 0)
                video_name = video_path.name
                
                # Státusz kód
                status_code = normalize_status_to_code(status_text)
                
                # Méretek
                if new_size_mb is not None:
                    new_size_str = f"{format_localized_number(new_size_mb, decimals=1)} MB"
                else:
                    new_size_str = "-"
                if change_percent is not None:
                    change_percent_str = f"{format_localized_number(change_percent, decimals=2, show_sign=True)}%"
                else:
                    change_percent_str = "-"
                
                # Bájt értékek számítása
                orig_size_bytes = parse_size_to_bytes(orig_size_str) if orig_size_str and orig_size_str != "-" else None
                new_size_bytes = int(new_size_mb * 1024 * 1024) if new_size_mb else None
                
                # Meglévő adatok megtartása (ha vannak)
                if existing_row:
                    # Meglévő értékek használata, ha nincs új érték
                    if not output_path_str:
                        output_path_str = existing_row[0]
                    if not order_num:
                        order_num = existing_row[1] or 0
                    if not video_name:
                        video_name = existing_row[2] or video_path.name
                    if orig_size_bytes is None:
                        orig_size_bytes = existing_row[3]
                    if new_size_bytes is None:
                        new_size_bytes = existing_row[4]
                    
                    source_frame_count = existing_row[5]
                    source_duration_seconds = existing_row[6]
                    source_fps = existing_row[7]
                    source_modified_timestamp = existing_row[8]
                    output_modified_timestamp = existing_row[9]
                    output_file_size_bytes = existing_row[10]
                    output_encoder_type = existing_row[11]
                else:
                    # Nincs meglévő bejegyzés - tree-ből vagy probolásból
                    # Tree item mögötti adatok használata
                    original_data = self.tree_item_data.get(item_id, {})
                    
                    source_frame_count = original_data.get('source_frame_count')
                    source_duration_seconds = original_data.get('source_duration_seconds')
                    source_fps = original_data.get('source_fps')
                    
                    # Source stat() - cache-ből vagy újra
                    cached_stat = self.video_stat_cache.get(video_path)
                    if cached_stat:
                        if orig_size_bytes is None:
                            orig_size_bytes = cached_stat.get('source_size_bytes')
                        source_modified_timestamp = cached_stat.get('source_modified_timestamp')
                    else:
                        if video_path.exists():
                            try:
                                stat_info = video_path.stat()
                                if orig_size_bytes is None:
                                    orig_size_bytes = stat_info.st_size
                                source_modified_timestamp = stat_info.st_mtime
                            except (OSError, PermissionError):
                                source_modified_timestamp = None
                        else:
                            source_modified_timestamp = None
                    
                    # Output fájl adatok (ha completed státusz)
                    if status_code in ('completed', 'completed_nvenc', 'completed_svt', 'completed_copy', 'completed_exists'):
                        if output_file and output_file.exists():
                            try:
                                output_stat_info = output_file.stat()
                                output_file_size_bytes = output_stat_info.st_size
                                output_modified_timestamp = output_stat_info.st_mtime
                            except (OSError, PermissionError):
                                output_file_size_bytes = None
                                output_modified_timestamp = None
                            
                            # Output encoder_type - tree item mögötti adatokból vagy probolásból
                            output_encoder_type = original_data.get('output_encoder_type')
                            if not output_encoder_type:
                                # Probolás szükséges
                                try:
                                    probe_cmd = [
                                        FFPROBE_PATH, '-v', 'error',
                                        '-show_entries', 'format_tags=Settings',
                                        '-of', 'default=noprint_wrappers=1:nokey=1',
                                        str(output_file)
                                    ]
                                    result = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=30, startupinfo=get_startup_info())
                                    if result.returncode == 0 and result.stdout:
                                        settings_tag = result.stdout.strip()
                                        if 'encoder=nvenc' in settings_tag.lower():
                                            output_encoder_type = 'nvenc'
                                        elif 'encoder=svt-av1' in settings_tag.lower() or 'encoder=libsvtav1' in settings_tag.lower():
                                            output_encoder_type = 'svt-av1'
                                except Exception:
                                    pass
                        else:
                            output_file_size_bytes = None
                            output_modified_timestamp = None
                            output_encoder_type = None
                    else:
                        output_file_size_bytes = None
                        output_modified_timestamp = None
                        output_encoder_type = None
                
                # INSERT OR REPLACE
                cursor.execute('''
                    INSERT OR REPLACE INTO videos (
                        video_path, output_path, order_number, video_name, status, status_code,
                        cq, vmaf, psnr, progress, orig_size, new_size, size_change, completed_date,
                        orig_size_bytes, new_size_bytes, source_frame_count, source_duration_seconds, source_fps,
                        source_modified_timestamp, output_modified_timestamp, output_file_size_bytes, output_encoder_type
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    video_path_str, output_path_str, order_num, video_name, status_text, status_code,
                    cq_str, vmaf_str, psnr_str, "100%", orig_size_str, new_size_str, change_percent_str, completed_date,
                    orig_size_bytes, new_size_bytes, source_frame_count, source_duration_seconds, source_fps,
                    source_modified_timestamp, output_modified_timestamp, output_file_size_bytes, output_encoder_type
                ))
                
                conn.commit()
                
                # Notification megjelenítése (debounce-olva, hogy ne jelenjen meg túl gyakran)
                self.root.after(0, self.show_db_update_notification_debounced)
                
            except (sqlite3.Error, OSError, PermissionError) as e:
                if conn:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                # Csendes hiba - ne zavarjuk meg az encoding folyamatot
                if LOG_WRITER:
                    try:
                        LOG_WRITER.write(f"⚠ [update_single_video_in_db] Hiba: {e} | video: {video_path}\n")
                        LOG_WRITER.flush()
                    except Exception:
                        pass
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass
    
    def _save_settings_debounced(self):
        """Debounced beállítások mentése (2 másodperc késleltetéssel)"""
        # Töröljük az előző timert, ha van
        if self.settings_save_timer:
            self.root.after_cancel(self.settings_save_timer)
        
        # Új timer beállítása 2 másodpercre
        self.settings_save_timer = self.root.after(2000, self._do_save_settings)
    
    def _do_save_settings(self):
        """Tényleges beállítások mentése háttérszálban"""
        def save_in_thread():
            try:
                self.save_settings_to_db()
            except Exception as e:
                if LOAD_DEBUG:
                    load_debug_log(f"[_do_save_settings] Hiba: {e}")
        
        threading.Thread(target=save_in_thread, daemon=True).start()
        self.settings_save_timer = None
    
    def load_state_from_db(self):
        """Táblázat állapot betöltése SQLite adatbázisból"""
        # Lock használata - biztosítja, hogy egyszerre csak egy adatbázis művelet fusson
        with self.db_lock:
            conn = None
            try:
                if not self.db_path.exists():
                    return None
                
                # Retry logika SQLITE_BUSY hibákra
                max_retries = 3
                retry_delay = 0.1  # 100ms
                for attempt in range(max_retries):
                    try:
                        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
                        break  # Sikeres kapcsolat
                    except sqlite3.OperationalError as e:
                        if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                            if LOAD_DEBUG:
                                load_debug_log(f"[load_state_from_db] Adatbázis lockolt, újrapróbálás {attempt + 1}/{max_retries}...")
                            time.sleep(retry_delay * (attempt + 1))  # Exponenciális backoff
                            continue
                        else:
                            raise  # Egyéb hiba vagy utolsó próbálkozás
                cursor = conn.cursor()
                self._ensure_db_tables(cursor)
                
                # Settings betöltése
                cursor.execute('SELECT key, value FROM settings')
                settings_rows = cursor.fetchall()
                settings_dict = {row[0]: row[1] for row in settings_rows}
                
                # Videos betöltése
                cursor.execute('''
                    SELECT video_path, output_path, order_number, video_name, status, status_code,
                           cq, vmaf, psnr, progress, orig_size, new_size, size_change, completed_date,
                           orig_size_bytes, new_size_bytes, source_frame_count, source_duration_seconds, source_fps,
                           source_modified_timestamp, output_modified_timestamp, output_file_size_bytes, output_encoder_type
                    FROM videos
                ''')
                videos_rows = cursor.fetchall()
                
                # Videos lista létrehozása
                videos_list = []
                for row in videos_rows:
                    video_dict = {
                        'video_path': row[0],
                        'output_path': row[1],
                        'order_number': row[2],
                        'video_name': row[3],
                        'status': row[4],
                        'status_code': row[5],
                        'cq': row[6],
                        'vmaf': row[7],
                        'psnr': row[8],
                        'progress': row[9],
                        'orig_size': row[10],
                        'new_size': row[11],
                        'size_change': row[12],
                        'completed_date': row[13],
                        'orig_size_bytes': row[14],
                        'new_size_bytes': row[15],
                        'source_frame_count': row[16],
                        'source_duration_seconds': row[17],
                        'source_fps': row[18],
                        'source_modified_timestamp': row[19],
                        'output_modified_timestamp': row[20],
                        'output_file_size_bytes': row[21],
                        'output_encoder_type': row[22]
                    }
                    videos_list.append(video_dict)
                
                # State data összeállítása (kompatibilitás a régi kóddal)
                state_data = {
                    'source_path': settings_dict.get('source_path'),
                    'dest_path': settings_dict.get('dest_path'),
                    'min_vmaf': float(settings_dict.get('min_vmaf', 0)) if settings_dict.get('min_vmaf') else 0,
                    'vmaf_step': float(settings_dict.get('vmaf_step', 0)) if settings_dict.get('vmaf_step') else 0,
                    'max_encoded_percent': int(settings_dict.get('max_encoded_percent', 0)) if settings_dict.get('max_encoded_percent') else 0,
                    'resize_enabled': settings_dict.get('resize_enabled') == 'True' if settings_dict.get('resize_enabled') else False,
                    'resize_height': int(settings_dict.get('resize_height', 0)) if settings_dict.get('resize_height') else 0,
                    'audio_compression_enabled': settings_dict.get('audio_compression_enabled') == 'True' if settings_dict.get('audio_compression_enabled') else False,
                    'audio_compression_method': settings_dict.get('audio_compression_method', ''),
                    'auto_vmaf_psnr': settings_dict.get('auto_vmaf_psnr') == 'True' if settings_dict.get('auto_vmaf_psnr') else False,
                    'svt_preset': int(settings_dict.get('svt_preset', 0)) if settings_dict.get('svt_preset') else 0,
                    'nvenc_worker_count': int(settings_dict.get('nvenc_worker_count', 0)) if settings_dict.get('nvenc_worker_count') else 0,
                    'videos': videos_list
                }
                
                return state_data
            except (sqlite3.Error, OSError, PermissionError, ValueError, TypeError) as e:
                if LOAD_DEBUG:
                    load_debug_log(f"[load_state_from_db] Hiba: {e}")
                return None
            except Exception as e:
                if LOAD_DEBUG:
                    load_debug_log(f"[load_state_from_db] Váratlan hiba: {e}")
                return None
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass
    
    def clear_table(self):
        """Táblázat teljes törlése"""
        result = messagebox.askyesno(
            t('btn_clear_table'),
            t('msg_clear_confirm')
        )
        
        if not result:
            return
        
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        self.video_items.clear()
        self.subtitle_items.clear()
        self.video_to_output.clear()
        self.video_files.clear()
        self.tree_item_data.clear()  # Tree item mögötti adatok törlése
        self.video_stat_cache.clear()  # Stat cache törlése
        
        # Adatbázis törlése
        if self.db_path.exists():
            try:
                self.db_path.unlink()
            except (OSError, PermissionError, FileNotFoundError):
                pass
        
        self.update_summary_row()
        self.status_label.config(text=t('btn_clear_table'))

    
    def update_summary_row(self):
        total_orig_size_bytes = 0
        total_new_size_bytes = 0
        encoded_count = 0
        
        for video_path, item_id in self.video_items.items():
            values = self.tree.item(item_id)['values']
            status = values[self.COLUMN_INDEX['status']] if len(values) > self.COLUMN_INDEX['status'] else ""
            if is_status_completed(status):
                try:
                    # Parse size strings to bytes (handles MB/GB/TB automatically via parse_size_to_bytes)
                    orig_size_str = values[self.COLUMN_INDEX['orig_size']] if len(values) > self.COLUMN_INDEX['orig_size'] else "-"
                    new_size_str = values[self.COLUMN_INDEX['new_size']] if len(values) > self.COLUMN_INDEX['new_size'] else "-"
                    
                    orig_bytes = parse_size_to_bytes(orig_size_str)
                    new_bytes = parse_size_to_bytes(new_size_str)
                    
                    # Ha a parse_size_to_bytes nem működik (pl. GB/TB esetén), próbáljuk manuálisan
                    if orig_bytes is None and orig_size_str != "-":
                        # Próbáljuk kinyerni a számot és az egységet
                        orig_bytes = self._parse_size_string_to_bytes(orig_size_str)
                    if new_bytes is None and new_size_str != "-":
                        new_bytes = self._parse_size_string_to_bytes(new_size_str)
                    
                    if orig_bytes is not None:
                        total_orig_size_bytes += orig_bytes
                    if new_bytes is not None:
                        total_new_size_bytes += new_bytes
                    
                    if orig_bytes is not None or new_bytes is not None:
                        encoded_count += 1
                except (ValueError, TypeError, AttributeError, IndexError):
                    pass
        
        for item in self.summary_tree.get_children():
            self.summary_tree.delete(item)
        
        if encoded_count > 0 and total_orig_size_bytes > 0:
            # Megjelenítjük az összesítő sort, ha van kész videó
            self.summary_frame.pack(fill=tk.X)
            change_percent = ((total_new_size_bytes - total_orig_size_bytes) / total_orig_size_bytes) * 100 if total_orig_size_bytes > 0 else 0
            orig_size_str = format_size_auto(total_orig_size_bytes)
            new_size_str = format_size_auto(total_new_size_bytes)
            change_percent_str = f"{format_localized_number(change_percent, decimals=2, show_sign=True)}%"
            self.summary_tree.insert("", tk.END, text="Σ",
                values=(f"━━━━ ÖSSZESÍTÉS ({encoded_count} videó) ━━━━", "", "", "", "", "", orig_size_str, new_size_str, change_percent_str, "", "", ""), tags=("summary",))
        else:
            # Elrejtjük az összesítő sort, ha nincs kész videó
            self.summary_frame.pack_forget()
    
    def check_and_fix_misnamed_copies(self):
        """Check and fix misnamed .av1.mkv copies after loading.
        
        Detects .av1.mkv files that are actually unchanged copies (same size as source)
        and renames them to original extension. Also ensures subtitles are copied.
        
        Returns:
            int: Number of files fixed.
        """
        fixed_count = 0
        
        for video_path, item_id in list(self.video_items.items()):
            output_file = self.video_to_output.get(video_path)
            
            if not output_file or not output_file.exists():
                continue
            
            if not is_misnamed_copy(video_path, output_file):
                continue
            
            if LOG_WRITER:
                try:
                    LOG_WRITER.write(f"\n🔄 Misnamed copy detected: {output_file.name}\n")
                    LOG_WRITER.flush()
                except (OSError, IOError):
                    pass
            
            new_output = rename_misnamed_copy_file(output_file, video_path)
            
            if not new_output:
                continue
            
            self.video_to_output[video_path] = new_output
            
            try:
                subs_copied = verify_and_copy_subtitles(video_path, new_output)
            except Exception:
                pass
            
            try:
                try:
                    orig_size_mb = video_path.stat().st_size / (1024**2)
                    new_size_mb = new_output.stat().st_size / (1024**2)
                    orig_size_display = f"{format_localized_number(orig_size_mb, decimals=1)} MB"
                    new_size_display = f"{format_localized_number(new_size_mb, decimals=1)} MB"
                except (OSError, ValueError):
                    orig_size_display = "-"
                    new_size_display = "-"
                
                completed_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                self.tree.set(item_id, 'status', t('status_completed_copy'))
                self.tree.set(item_id, 'orig_size', orig_size_display)
                self.tree.set(item_id, 'new_size', new_size_display)
                self.tree.set(item_id, 'size_change', "0%")
                self.tree.set(item_id, 'completed_date', completed_date)
                self.tree.item(item_id, tags=('completed',))
                
                if item_id in self.tree_item_data:
                    self.tree_item_data[item_id]['status_code'] = 'completed_copy'
            except (tk.TclError, KeyError, AttributeError):
                pass
            
            fixed_count += 1
        
        return fixed_count
    
    def _parse_size_string_to_bytes(self, size_str):
        """Parse size string to bytes, handling MB, GB, TB units"""
        if not size_str or size_str == "-":
            return None
        try:
            # Remove spaces and convert to uppercase for easier matching
            clean = size_str.strip().upper()
            
            # Try to extract number and unit
            # Handle localized format (comma as decimal separator)
            clean_normalized = clean.replace(',', '.')
            
            # Match patterns like "123.45 MB", "1,234.56 GB", etc.
            match = re.match(r'([\d.,]+)\s*(KB|MB|GB|TB|B)', clean_normalized)
            if match:
                number_str = match.group(1).replace(',', '')
                unit = match.group(2).upper()
                number = float(number_str)
                
                if unit == 'TB':
                    return int(number * (1024 ** 4))
                elif unit == 'GB':
                    return int(number * (1024 ** 3))
                elif unit == 'MB':
                    return int(number * (1024 ** 2))
                elif unit == 'KB':
                    return int(number * 1024)
                elif unit == 'B':
                    return int(number)
            
            # Fallback: try to parse as MB (old format)
            clean = clean.replace("MB", "").replace("GB", "").replace("TB", "").replace("KB", "").replace("B", "").strip()
            clean = clean.replace(',', '.')
            value = float(clean)
            # Assume MB if no unit found
            return int(value * (1024 ** 2))
        except (ValueError, TypeError, AttributeError):
            return None
            
    def load_videos(self):
        """Load videos from the source directory.
        
        Scans the source directory for video files, checks their status in the database,
        and populates the GUI treeview. Handles both cold start (no DB) and warm start.
        """
        # Ha Start gomb helyett Leállítás gomb látszik, akkor inaktív legyen a videók betöltése gomb
        # (A gomb állapotát máshol kezeljük, itt csak biztonsági ellenőrzés)
        if self.is_encoding:
            # Ha fut a kódolás, akkor nem engedélyezzük a betöltést
            return

        def log_file_check(msg):
            if LOG_WRITER:
                try:
                    LOG_WRITER.write(msg + "\n")
                    LOG_WRITER.flush()
                except (OSError, IOError, AttributeError):
                    pass

        def format_relative_name(video_path):
            try:
                rel = video_path.resolve().relative_to(self.source_path.resolve())
                return str(rel)
            except Exception as e:
                log_file_check(f"✗ Relatív útvonal hiba: {video_path} -> {e}")
                return str(video_path)
        
        # format_relative_name elérhetővé tétele osztály szinten
        self.format_relative_name = format_relative_name
        
        def build_load_error_result(video_path, error_message):
            """Segédfüggvény: hiba esetén is adjunk vissza megjeleníthető sort."""
            if video_path:
                try:
                    video_name = self.format_relative_name(video_path)
                except Exception:
                    video_name = str(video_path)
                order_num = self.video_order.get(video_path, 0) if hasattr(self, 'video_order') else 0
            else:
                video_name = "Ismeretlen videó"
                order_num = 0
            
            error_text = str(error_message) if error_message else ""
            if len(error_text) > 120:
                error_text = error_text[:117] + "..."
            status_text = f"{t('status_load_error')}: {error_text}" if error_text else t('status_load_error')
            load_debug_log(f"Hibasor létrehozva: {video_name} -> {status_text}")
            result = {
                'video_path': video_path,
                'order_num': order_num,
                'video_name': video_name,
                'values': (video_name, status_text, "-", "-", "-", "-", "-", "-", "-", "-", "-", ""),
                'tag': 'failed',
                'error': error_text
            }
            if hasattr(self, 'last_load_errors'):
                try:
                    self.last_load_errors.append((video_name, status_text))
                except Exception:
                    pass
            return result
        
        source = self.source_entry.get()
        if not source or not os.path.exists(source):
            messagebox.showerror("Hiba", t('msg_invalid_source'))
            return
        
        self.source_path = Path(source)
        dest = self.dest_entry.get()
        self.dest_path = Path(dest) if dest else None
        destination_is_empty = False
        if self.dest_path:
            video_loading_log(f"STEP 1.5: Cél dir lista ellenőrzése")
            dest_dir_start = time.time()
            destination_is_empty = is_directory_completely_empty(self.dest_path)
            dest_dir_time = (time.time() - dest_dir_start) * 1000
            video_loading_log(f"STEP 1.5 DONE: {dest_dir_time:.2f}ms - destination_is_empty={destination_is_empty}")
            if destination_is_empty:
                log_file_check("⚠ Célmappa üres → meglévő kimenetek ellenőrzését kihagyjuk.")
                video_loading_log(f"  Célmappa üres → meglévő kimenetek ellenőrzését kihagyjuk")
        
        # Adatbázis állapot ellenőrzése és felajánlása (csak akkor, ha nem indítási betöltés)
        video_loading_log(f"STEP 2: DB query (load_state_from_db)")
        db_query_start = time.time()
        saved_state = self.load_state_from_db()
        db_query_time = (time.time() - db_query_start) * 1000
        video_loading_log(f"STEP 2 DONE: {db_query_time:.2f}ms - saved_state={'YES' if saved_state else 'NO'}")
        
        # DB tartalom JSON formátumban a log fájlba
        if saved_state:
            # Összefoglaló statisztikák
            db_summary = {
                'db_path': str(self.db_path),
                'settings': {
                    'source_path': saved_state.get('source_path'),
                    'dest_path': saved_state.get('dest_path'),
                    'min_vmaf': saved_state.get('min_vmaf'),
                    'vmaf_step': saved_state.get('vmaf_step'),
                    'max_encoded_percent': saved_state.get('max_encoded_percent'),
                    'resize_enabled': saved_state.get('resize_enabled'),
                    'resize_height': saved_state.get('resize_height'),
                    'audio_compression_enabled': saved_state.get('audio_compression_enabled'),
                    'audio_compression_method': saved_state.get('audio_compression_method'),
                    'auto_vmaf_psnr': saved_state.get('auto_vmaf_psnr'),
                    'svt_preset': saved_state.get('svt_preset'),
                    'nvenc_worker_count': saved_state.get('nvenc_worker_count')
                },
                'videos_count': len(saved_state.get('videos', [])),
                'videos_sample': saved_state.get('videos', [])[:10] if len(saved_state.get('videos', [])) > 10 else saved_state.get('videos', []),
                'videos_full': saved_state.get('videos', [])  # Teljes lista
            }
            video_loading_log_json(db_summary, "Database Content (Full)")
            
            # Rövidebb összefoglaló is (statisztikák)
            stats = {
                'total_videos': len(saved_state.get('videos', [])),
                'videos_with_source_size': sum(1 for v in saved_state.get('videos', []) if v.get('orig_size_bytes')),
                'videos_with_source_timestamp': sum(1 for v in saved_state.get('videos', []) if v.get('source_modified_timestamp')),
                'videos_with_output': sum(1 for v in saved_state.get('videos', []) if v.get('output_path')),
                'videos_by_status': {}
            }
            for video in saved_state.get('videos', []):
                status = video.get('status', 'unknown')
                stats['videos_by_status'][status] = stats['videos_by_status'].get(status, 0) + 1
            video_loading_log_json(stats, "Database Statistics")
        else:
            video_loading_log_json({'db_path': str(self.db_path), 'status': 'no_data'}, "Database Content")
        
        load_saved = False
        
        if saved_state:
            # Ellenőrizzük, hogy ugyanazok-e a forrás/cél útvonalak
            saved_source = saved_state.get('source_path')
            saved_dest = saved_state.get('dest_path')
            
            video_loading_log(f"  DB saved_source: {saved_source}")
            video_loading_log(f"  Current source_path: {self.source_path}")
            video_loading_log(f"  DB saved_dest: {saved_dest}")
            video_loading_log(f"  Current dest_path: {self.dest_path}")
            
            # Path összehasonlítás normalizálva (resolve() hogy abszolút útvonalak legyenek)
            try:
                saved_source_path = Path(saved_source).resolve() if saved_source else None
                current_source_path = self.source_path.resolve() if self.source_path else None
                source_match = saved_source_path and current_source_path and saved_source_path == current_source_path
            except Exception as e:
                video_loading_log(f"  ERROR source path comparison: {e}")
                source_match = False
            
            try:
                saved_dest_path = Path(saved_dest).resolve() if saved_dest else None
                current_dest_path = self.dest_path.resolve() if self.dest_path else None
                dest_match = (not saved_dest_path and not current_dest_path) or (saved_dest_path and current_dest_path and saved_dest_path == current_dest_path)
            except Exception as e:
                video_loading_log(f"  ERROR dest path comparison: {e}")
                dest_match = False
            
            video_loading_log(f"  source_match: {source_match} (saved={saved_source}, current={self.source_path})")
            video_loading_log(f"  dest_match: {dest_match} (saved={saved_dest}, current={self.dest_path})")
            
            # OPTIMALIZÁCIÓ: Még ha a path-ok nem egyeznek is, betöltjük a videó adatokat!
            # Mert lehet, hogy ugyanazok a fájlok, csak más útvonalon vannak (pl. másik meghajtó)
            # A videó adatok betöltésekor normalizált útvonalakkal hasonlítunk, így megtaláljuk őket
            if source_match and dest_match:
                # Ha a path-ok is egyeznek, akkor biztosan betöltjük
                load_saved = True
                video_loading_log(f"  ✓ load_saved = True (paths match)")
            elif saved_state.get('videos'):
                # Ha a path-ok nem egyeznek, DE vannak videó adatok a DB-ben, akkor is betöltjük
                # (mert lehet, hogy ugyanazok a fájlok, csak más útvonalon)
                load_saved = True
                video_loading_log(f"  ✓ load_saved = True (paths don't match, but videos exist in DB - will try to match by normalized paths)")
            else:
                video_loading_log(f"  ✗ load_saved = False (no videos in DB)")
        else:
            video_loading_log(f"  ✗ load_saved = False (no saved_state)")
        
        log_file_check(f"\n=== VIDEÓK BETÖLTÉSE ===")
        log_file_check(f"Forrás: {source}")
        log_file_check(f"Cél: {self.dest_entry.get() or 'N/A'}")
        
        video_loading_log(f"=== VIDEÓK BETÖLTÉSE KEZDETE ===")
        video_loading_log(f"Forrás: {source}")
        video_loading_log(f"Cél: {self.dest_entry.get() or 'N/A'}")

        self.is_loading_videos = True
        self.last_load_errors = []
        self.update_start_button_state()

        for item in self.tree.get_children():
            self.tree.delete(item)
        
        self.video_items.clear()
        self.subtitle_items.clear()
        self.video_to_output.clear()
        self.tree_item_data.clear()  # Tree item mögötti adatok törlése
        self.video_stat_cache.clear()  # Stat cache törlése
        
        # Ha a skip_av1_files checkbox be van pipálva, az .av1 fájlokat külön kezeljük
        skip_av1 = self.skip_av1_files.get()
        
        # Normál videó fájlok betöltése (.av1 kimeneteket mindig kihagyjuk az encode queue-ból)
        video_loading_log(f"STEP 1: Forrás dir lista (find_video_files)")
        dir_list_start = time.time()
        self.video_files = find_video_files(self.source_path, include_av1=False)
        dir_list_time = (time.time() - dir_list_start) * 1000
        video_loading_log(f"STEP 1 DONE: {dir_list_time:.2f}ms - {len(self.video_files)} videó találva")
        
        # Ha skip_av1 be van pipálva, az .av1 fájlokat másoljuk át (ha van cél mappa)
        if skip_av1 and self.dest_path:
            av1_files = []
            root_path = Path(self.source_path)
            for file_path in root_path.rglob('*'):
                if file_path.is_file() and file_path.suffix.lower() in VIDEO_EXTENSIONS:
                    # Kihagyjuk a .ab-av1-* almappákban lévő fájlokat (ab-av1 temp fájlok)
                    path_parts = file_path.parts
                    if any('.ab-av1-' in part for part in path_parts):
                        continue
                    if file_path.stem.endswith('.av1'):
                        av1_files.append(file_path)
            
            # Másoljuk az .av1 fájlokat
            for av1_file in av1_files:
                relative_path = av1_file.relative_to(self.source_path)
                dest_file = self.dest_path / relative_path
                
                # Ha már létezik, kihagyjuk
                if dest_file.exists():
                    continue
                
                try:
                    dest_file.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(av1_file, dest_file)
                    # Feliratok másolása is
                    subtitle_files = find_subtitle_files(av1_file)
                    for sub_path, lang_part in subtitle_files:
                        dest_sub_name = dest_file.stem
                        if lang_part:
                            dest_sub_name += f".{lang_part}"
                        dest_sub_name += sub_path.suffix
                        dest_sub_path = dest_file.parent / dest_sub_name
                        if not dest_sub_path.exists():
                            shutil.copy2(sub_path, dest_sub_path)
                except Exception as e:
                    log_file_check(f"✗ Hiba az .av1 fájl másolásakor ({av1_file.name}): {e}")
                    print(f"✗ Hiba az .av1 fájl másolásakor ({av1_file.name}): {e}")
        
        log_file_check(f"Talált videófájlok száma: {len(self.video_files)} (skip_av1={self.skip_av1_files.get()})")

        if not self.video_files:
            log_file_check("✗ Nincs betölthető videó a forrás mappában.")
            messagebox.showinfo("Info", t('msg_no_video'))
            self.is_loading_videos = False
            self.update_start_button_state()
            return
        
        # ABC sorrendbe rendezés relatív útvonal alapján (állandó sorrend)
        def get_sort_key(video_path):
            """Relatív útvonal alapján rendezés"""
            try:
                rel = video_path.resolve().relative_to(self.source_path.resolve())
                return str(rel).lower()  # Case-insensitive rendezés
            except Exception:
                return str(video_path).lower()
        
        # Rendezzük ABC sorrendbe
        self.video_files.sort(key=get_sort_key)
        
        # Sorszámok beállítása ABC sorrendben (állandó, nem változik)
        self.video_order = {}
        for idx, video_path in enumerate(self.video_files, 1):
            self.video_order[video_path] = idx
        
        # Probe-olás előkészítése (mindig szükséges)
        video_loading_log(f"STEP 3: Adatok összevetése és feldolgozás előkészítése")
        video_loading_log(f"  load_saved={load_saved}, saved_state={'YES' if saved_state else 'NO'}")
        comparison_start = time.time()
        saved_videos = {}
        saved_videos_by_name = {}  # Fallback: név alapján is kereshetünk
        if load_saved and saved_state:
            videos_list = saved_state.get('videos', [])
            video_loading_log(f"  DB-ből {len(videos_list)} videó adat érkezett")
            saved_videos = {}
            saved_videos_by_name = {}
            for v in videos_list:
                try:
                    video_path_str = v.get('video_path')
                    if video_path_str:
                        # Normalizáljuk az útvonalat (resolve() hogy abszolút legyen)
                        video_path_db = Path(video_path_str).resolve()
                        saved_videos[video_path_db] = v
                        # Fallback: név alapján is indexeljük (relatív útvonal vagy fájlnév)
                        try:
                            video_name_db = v.get('video_name') or Path(video_path_str).name
                            if video_name_db:
                                saved_videos_by_name[video_name_db] = v
                        except Exception:
                            pass
                except Exception as e:
                    video_loading_log(f"  ERROR converting video_path to Path: {video_path_str} -> {e}")
            video_loading_log(f"  {len(saved_videos)} videó adat konvertálva Path objektumokká")
            video_loading_log(f"  {len(saved_videos_by_name)} videó adat indexelve név alapján (fallback)")
            
            # Teszt: nézzük meg, hogy az első néhány videó megtalálható-e
            if self.video_files and saved_videos:
                test_count = min(5, len(self.video_files))
                video_loading_log(f"  Teszt: első {test_count} videó egyezés ellenőrzése:")
                for i, video_path in enumerate(list(self.video_files)[:test_count]):
                    try:
                        normalized_video_path = video_path.resolve()
                        found = normalized_video_path in saved_videos
                        video_loading_log(f"    [{i+1}] {video_path.name[:50]}... -> {'✓ FOUND' if found else '✗ NOT FOUND'}")
                        if not found:
                            # Próbáljuk meg a DB-ben lévő útvonalakkal is összevetni
                            for db_path in list(saved_videos.keys())[:10]:  # Csak első 10-et nézzük meg
                                try:
                                    if db_path.resolve() == normalized_video_path:
                                        video_loading_log(f"      -> ✓ FOUND (normalized path match with {db_path})")
                                        found = True
                                        break
                                except Exception:
                                    pass
                    except Exception as e:
                        video_loading_log(f"    [{i+1}] ERROR: {e}")
        else:
            video_loading_log(f"  ✗ Nincs saved_videos (load_saved={load_saved}, saved_state={'YES' if saved_state else 'NO'})")
        comparison_time = (time.time() - comparison_start) * 1000
        video_loading_log(f"STEP 3 DONE: {comparison_time:.2f}ms - {len(saved_videos)} videó adat kész használatra")
        
        # Ha betöltjük a mentett állapotot
        if load_saved and saved_state:
            # VMAF beállítások visszaállítása
            if 'min_vmaf' in saved_state:
                self.update_vmaf_label(saved_state['min_vmaf'])
            if 'vmaf_step' in saved_state:
                self.update_vmaf_step_label(saved_state['vmaf_step'])
            if 'max_encoded_percent' in saved_state:
                self.update_max_encoded_label(saved_state['max_encoded_percent'])
            if 'resize_enabled' in saved_state:
                self.resize_enabled.set(saved_state['resize_enabled'])
                self.toggle_resize_slider()
            if 'resize_height' in saved_state:
                self.update_resize_label(saved_state['resize_height'])
            if 'audio_compression_enabled' in saved_state:
                self.audio_compression_enabled.set(saved_state['audio_compression_enabled'])
            if 'audio_compression_method' in saved_state:
                method = saved_state['audio_compression_method']
                self.audio_compression_method.set(method)
                # Combobox érték frissítése
                if hasattr(self, 'audio_compression_combo'):
                    if method == 'fast':
                        self.audio_compression_combo.set(t('audio_compression_fast'))
                    elif method == 'dialogue':
                        self.audio_compression_combo.set(t('audio_compression_dialogue'))
            if 'svt_preset' in saved_state:
                svt_preset_val = int(saved_state['svt_preset']) if saved_state['svt_preset'] else 2
                self.svt_preset.set(svt_preset_val)
                self.svt_preset_value_label.config(text=str(svt_preset_val))
            if 'nvenc_worker_count' in saved_state:
                self.nvenc_worker_count.set(int(saved_state['nvenc_worker_count']))
                self.update_nvenc_workers_label(saved_state['nvenc_worker_count'])
            
            # Ne használjuk a JSON-ból betöltött sorszámokat, mert az ABC sorrend állandó
            # A video_files már ABC sorrendben van, és a video_order is ABC sorrendben van beállítva
            # Ez biztosítja, hogy mindig ugyanaz a sorszám legyen ugyanaz a fájlhoz
        
        # Párhuzamos videó adatgyűjtés helper függvény
        def process_video_data(video_path):
            """
            Egy videó összes adatának összegyűjtése párhuzamosan
            """
            video_loading_log(f"START process_video_data: {video_path}")
            result = {
                'video_path': video_path,
                'order_num': self.video_order.get(video_path, 0),
                'video_name': None,
                'exists': False,
                'source_size_bytes': None,
                'source_frame_count': None,
                'source_duration_seconds': None,
                'output_file': None,
                'output_exists': False,
                'output_info': None,
                'saved_video': None,  # Később keressük meg normalizálva
                'subtitle_files': [],
                'values': None,
                'tag': None,
                'error': None
            }
            
            try:
                # Video name
                try:
                    rel = video_path.resolve().relative_to(self.source_path.resolve())
                    result['video_name'] = str(rel)
                except Exception:
                    result['video_name'] = str(video_path)
                video_loading_log(f"  video_name: {result['video_name']}")
                
                # Saved video keresése normalizált útvonallal
                try:
                    normalized_video_path = video_path.resolve()
                    result['saved_video'] = saved_videos.get(normalized_video_path)
                    if not result['saved_video']:
                        # Fallback: próbáljuk meg név alapján is
                        result['saved_video'] = saved_videos_by_name.get(result['video_name'])
                        if result['saved_video']:
                            video_loading_log(f"  Found saved_video by name (fallback)")
                    if result['saved_video']:
                        video_loading_log(f"  Found saved_video in DB")
                except Exception as e:
                    video_loading_log(f"  ERROR resolving video_path for saved_video lookup: {e}")
                    result['saved_video'] = saved_videos_by_name.get(result['video_name'])
                
                # Exists check - optimalizálva: csak akkor ellenőrizzük, ha nincs saved video (gyors betöltés)
                saved_video_check = result['saved_video']
                if saved_video_check:
                    # Ha van saved video, feltételezzük, hogy létezik (gyors)
                    result['exists'] = True
                else:
                    # Csak akkor ellenőrizzük, ha nincs saved video
                    if not video_path.exists():
                        result['values'] = (result['video_name'], t('status_source_missing'), "-", "-", "-", "-", "-", "-", "-", "-", "-", "")
                        result['tag'] = "failed"
                        return result
                    result['exists'] = True
                
                # Output file path
                result['output_file'] = get_output_filename(video_path, self.source_path, self.dest_path)
                
                # Saved video handling
                saved_video = result['saved_video']
                if saved_video:
                    video_loading_log(f"  HAS saved_video in DB")
                    # Custom output path
                    saved_output_path = saved_video.get('output_path')
                    if saved_output_path:
                        try:
                            result['output_file'] = Path(saved_output_path)
                            video_loading_log(f"  saved output_path: {saved_output_path}")
                        except (OSError, ValueError):
                            pass
                    
                    # Source size és dátum ellenőrzése - optimalizálva: egyetlen stat() hívás
                    saved_source_size_bytes = saved_video.get('orig_size_bytes')
                    saved_source_modified_timestamp = saved_video.get('source_modified_timestamp')
                    saved_status_code_for_source = saved_video.get('status_code')
                    if not saved_status_code_for_source:
                        saved_status_for_source = saved_video.get('status', '')
                        saved_status_code_for_source = normalize_status_to_code(saved_status_for_source)
                    
                    video_loading_log(f"  saved: size={saved_source_size_bytes}, date={saved_source_modified_timestamp}, status={saved_status_code_for_source}")
                    
                    # Gyors ellenőrzés: ha van saved adat (méret ÉS dátum), ellenőrizzük egyetlen stat() hívással
                    source_size_current = None
                    source_modified_current = None
                    file_size_matches = False
                    file_date_matches = False
                    should_skip_probe = False
                    
                    if saved_source_size_bytes is not None and saved_source_modified_timestamp is not None:
                        # Van saved méret ÉS dátum: gyors stat() ellenőrzés (egyetlen hívás)
                        video_loading_log(f"  Checking: saved size AND date exist, doing stat()")
                        try:
                            stat_start = time.time()
                            stat_info = video_path.stat()
                            stat_time = (time.time() - stat_start) * 1000
                            source_size_current = stat_info.st_size
                            source_modified_current = stat_info.st_mtime
                            
                            # Format dates for human-readable output
                            saved_date_str = datetime.fromtimestamp(saved_source_modified_timestamp).strftime('%Y-%m-%d %H:%M:%S')
                            current_date_str = datetime.fromtimestamp(source_modified_current).strftime('%Y-%m-%d %H:%M:%S')
                            
                            video_loading_log(f"  stat() took {stat_time:.2f}ms")
                            video_loading_log(f"  File size: {source_size_current:,} bytes | DB size: {saved_source_size_bytes:,} bytes")
                            video_loading_log(f"  File date: {current_date_str} | DB date: {saved_date_str}")
                            
                            # Összehasonlítás: ha mindkettő egyezik, használjuk a saved adatokat (gyors betöltés)
                            file_size_matches = (saved_source_size_bytes == source_size_current)
                            # Dátum összehasonlítás: kis tolerancia (1 másodperc) a fájlrendszer pontatlansága miatt
                            file_date_matches = abs(saved_source_modified_timestamp - source_modified_current) < 1.0
                            
                            # CSAK akkor skip-eljük a probot, ha MINDKETTŐ (méret ÉS dátum) egyezik
                            if file_size_matches and file_date_matches:
                                # Mindkettő egyezik: használjuk a saved értékeket (gyors) - NEM probolunk
                                result['source_size_bytes'] = saved_source_size_bytes
                                should_skip_probe = True
                                video_loading_log(f"  ✓ SKIP PROBE: File size = DB size ({source_size_current:,} bytes) AND File date = DB date ({current_date_str}) - No probing needed")
                            else:
                                # Méret vagy dátum eltér: frissítjük az értékeket és probolni kell
                                result['source_size_bytes'] = source_size_current
                                should_skip_probe = False
                                if not file_size_matches and not file_date_matches:
                                    video_loading_log(f"  ✗ NEED PROBE: File size changed ({source_size_current:,} bytes != {saved_source_size_bytes:,} bytes) AND File date changed ({current_date_str} != {saved_date_str})")
                                elif not file_size_matches:
                                    video_loading_log(f"  ✗ NEED PROBE: File size changed ({source_size_current:,} bytes != {saved_source_size_bytes:,} bytes)")
                                else:
                                    video_loading_log(f"  ✗ NEED PROBE: File date changed ({current_date_str} != {saved_date_str})")
                        except (OSError, PermissionError) as e:
                            # Fájl nem elérhető: használjuk a saved értékeket (gyors)
                            result['source_size_bytes'] = saved_source_size_bytes
                            should_skip_probe = True
                            video_loading_log(f"  ERROR stat(): {e}, using saved data")
                    elif saved_source_size_bytes is not None:
                        # Van csak saved méret (nincs dátum): gyors stat() ellenőrzés
                        video_loading_log(f"  Checking: saved size exists (no date in DB), doing stat()")
                        try:
                            stat_start = time.time()
                            stat_info = video_path.stat()
                            stat_time = (time.time() - stat_start) * 1000
                            source_size_current = stat_info.st_size
                            source_modified_current = stat_info.st_mtime
                            
                            # Format date for human-readable output
                            current_date_str = datetime.fromtimestamp(source_modified_current).strftime('%Y-%m-%d %H:%M:%S')
                            
                            video_loading_log(f"  stat() took {stat_time:.2f}ms")
                            video_loading_log(f"  File size: {source_size_current:,} bytes | DB size: {saved_source_size_bytes:,} bytes")
                            video_loading_log(f"  File date: {current_date_str} | DB date: (not available)")
                            
                            file_size_matches = (saved_source_size_bytes == source_size_current)
                            if file_size_matches:
                                # Méret egyezik: használjuk a saved értéket (gyors)
                                result['source_size_bytes'] = saved_source_size_bytes
                                result['source_modified_timestamp'] = source_modified_current  # Cache-eléshez (nincs DB-ben, de stat()-oltunk)
                                should_skip_probe = True
                                video_loading_log(f"  ✓ SKIP PROBE: File size = DB size ({source_size_current:,} bytes) - No probing needed (date not checked, no date in DB)")
                            else:
                                # Méret eltér: frissítjük
                                result['source_size_bytes'] = source_size_current
                                result['source_modified_timestamp'] = source_modified_current  # Cache-eléshez
                                should_skip_probe = False
                                video_loading_log(f"  ✗ NEED PROBE: File size changed ({source_size_current:,} bytes != {saved_source_size_bytes:,} bytes)")
                        except (OSError, PermissionError) as e:
                            result['source_size_bytes'] = saved_source_size_bytes
                            should_skip_probe = True
                            video_loading_log(f"  ERROR stat(): {e}, using saved data")
                    else:
                        # Nincs saved adat: beolvassuk (ritka eset)
                        video_loading_log(f"  No saved data in DB, reading from file")
                        try:
                            stat_start = time.time()
                            stat_info = video_path.stat()
                            stat_time = (time.time() - stat_start) * 1000
                            source_size_current = stat_info.st_size
                            source_modified_current = stat_info.st_mtime
                            result['source_size_bytes'] = source_size_current
                            result['source_modified_timestamp'] = source_modified_current  # Cache-eléshez
                            should_skip_probe = False
                            
                            # Format date for human-readable output
                            current_date_str = datetime.fromtimestamp(source_modified_current).strftime('%Y-%m-%d %H:%M:%S')
                            
                            video_loading_log(f"  stat() took {stat_time:.2f}ms")
                            video_loading_log(f"  File size: {source_size_current:,} bytes | DB size: (not available)")
                            video_loading_log(f"  File date: {current_date_str} | DB date: (not available)")
                            video_loading_log(f"  ✗ NEED PROBE: New video (no data in DB)")
                        except (OSError, PermissionError) as e:
                            result['source_size_bytes'] = None
                            should_skip_probe = False
                            video_loading_log(f"  ERROR stat(): {e}")
                else:
                    video_loading_log(f"  NO saved_video in DB (new video)")
                    # Nincs saved video, új videó - probolni kell
                    should_skip_probe = False
                    try:
                        stat_start = time.time()
                        stat_info = video_path.stat()
                        stat_time = (time.time() - stat_start) * 1000
                        result['source_size_bytes'] = stat_info.st_size
                        result['source_modified_timestamp'] = stat_info.st_mtime  # Cache-eléshez
                        
                        # Format date for human-readable output
                        current_date_str = datetime.fromtimestamp(stat_info.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                        
                        video_loading_log(f"  stat() took {stat_time:.2f}ms")
                        video_loading_log(f"  File size: {result['source_size_bytes']:,} bytes | DB size: (not available)")
                        video_loading_log(f"  File date: {current_date_str} | DB date: (not available)")
                        video_loading_log(f"  ✗ NEED PROBE: New video (no data in DB)")
                    except (OSError, PermissionError) as e:
                        result['source_size_bytes'] = None
                        video_loading_log(f"  ERROR stat(): {e}")
                
                # Probe results: használjuk a saved értékeket, ha a fájl nem módosult (gyors betöltés)
                if saved_video:
                    saved_frame_count = saved_video.get('source_frame_count')
                    saved_duration = saved_video.get('source_duration_seconds')
                    saved_fps = saved_video.get('source_fps')
                    
                    if should_skip_probe:
                        # Fájl nem módosult: használjuk a saved értékeket (gyors) - NEM probolunk
                        result['source_frame_count'] = saved_frame_count
                        result['source_duration_seconds'] = saved_duration
                        if saved_fps is not None:
                            result['source_fps'] = saved_fps
                        video_loading_log(f"  Using saved probe data: frames={saved_frame_count}, duration={saved_duration}, fps={saved_fps}")
                    else:
                        # Fájl módosult vagy nincs saved adat: probolni kell (ritka eset)
                        video_loading_log(f"  PROBING source video (file changed or no saved data)")
                        try:
                            probe_start = time.time()
                            duration, fps = get_video_info(video_path)
                            probe_time = (time.time() - probe_start) * 1000
                            if duration and fps:
                                result['source_frame_count'] = int(duration * fps)
                            result['source_duration_seconds'] = duration
                            result['source_fps'] = fps
                            video_loading_log(f"  Probe took {probe_time:.2f}ms: duration={duration}, fps={fps}, frames={result.get('source_frame_count')}")
                        except Exception as e:
                            # Fallback to saved values
                            result['source_frame_count'] = saved_frame_count
                            result['source_duration_seconds'] = saved_duration
                            if saved_fps is not None:
                                result['source_fps'] = saved_fps
                            video_loading_log(f"  Probe ERROR: {e}, using saved data")
                    
                    # Process saved video data (similar to original code)
                    # Először próbáljuk meg a source_size_bytes-ból számolni
                    if result['source_size_bytes']:
                        orig_size_str = format_size_mb(result['source_size_bytes'])
                    else:
                        # Ha nincs source_size_bytes, próbáljuk meg a saved orig_size stringet használni
                        saved_orig_size = saved_video.get('orig_size', '-')
                        if saved_orig_size != "-" and "MB" in saved_orig_size:
                            try:
                                size_num = float(saved_orig_size.replace("MB", "").strip())
                                orig_size_str = f"{format_localized_number(size_num, decimals=1)} MB"
                            except (ValueError, TypeError):
                                orig_size_str = saved_orig_size
                        else:
                            orig_size_str = saved_orig_size
                        
                        # Ha még mindig nincs érték, próbáljuk meg a korábban beolvasott source_size_current értéket használni
                        if (orig_size_str == "-" or not orig_size_str) and source_size_current is not None:
                            result['source_size_bytes'] = source_size_current
                            orig_size_str = format_size_mb(source_size_current)
                        elif orig_size_str == "-" or not orig_size_str:
                            # Utolsó esély: fájlból beolvasás (ritka eset, ha még nem olvastuk be)
                            try:
                                file_size_bytes = video_path.stat().st_size
                                result['source_size_bytes'] = file_size_bytes
                                orig_size_str = format_size_mb(file_size_bytes)
                            except (OSError, PermissionError):
                                pass
                    
                    # Output file check - optimalizálva: egyetlen stat() hívás, ha szükséges
                    if destination_is_empty:
                        result['output_exists'] = False
                        result['output_size_matches'] = False
                    else:
                        saved_status_code_check = saved_video.get('status_code')
                        if not saved_status_code_check:
                            saved_status_check = saved_video.get('status', '')
                            saved_status_code_check = normalize_status_to_code(saved_status_check)
                        
                        saved_modified = saved_video.get('output_modified_timestamp')
                        saved_file_size = saved_video.get('output_file_size_bytes')
                        
                        # Optimalizálás: ha van saved adat (méret ÉS dátum), ellenőrizzük egyetlen stat() hívással
                        if saved_status_code_check in ('completed', 'completed_nvenc', 'completed_svt', 'completed_copy', 'completed_exists'):
                            # Completed státusz: ha van saved adat, ellenőrizzük (gyors stat())
                            if saved_modified is not None and saved_file_size is not None:
                                # Van saved méret ÉS dátum: gyors stat() ellenőrzés (egyetlen hívás)
                                try:
                                    if result['output_file'] and result['output_file'].exists():
                                        output_stat = result['output_file'].stat()
                                        output_size_current = output_stat.st_size
                                        output_modified_current = output_stat.st_mtime
                                        
                                        # Format dates for human-readable output
                                        saved_date_str = datetime.fromtimestamp(saved_modified).strftime('%Y-%m-%d %H:%M:%S')
                                        current_date_str = datetime.fromtimestamp(output_modified_current).strftime('%Y-%m-%d %H:%M:%S')
                                        
                                        video_loading_log(f"  Output file check:")
                                        video_loading_log(f"    File size: {output_size_current:,} bytes | DB size: {saved_file_size:,} bytes")
                                        video_loading_log(f"    File date: {current_date_str} | DB date: {saved_date_str}")
                                        
                                        # Összehasonlítás: CSAK akkor skip-eljük a probot, ha MINDKETTŐ (méret ÉS dátum) egyezik
                                        output_size_matches = (saved_file_size == output_size_current)
                                        # Dátum összehasonlítás: kis tolerancia (1 másodperc)
                                        output_date_matches = abs(saved_modified - output_modified_current) < 1.0
                                        
                                        if output_size_matches and output_date_matches:
                                            # Mindkettő egyezik: használjuk a saved adatokat (gyors) - NEM probolunk
                                            result['output_exists'] = True
                                            result['output_size_matches'] = True
                                            video_loading_log(f"    ✓ SKIP OUTPUT PROBE: File size = DB size ({output_size_current:,} bytes) AND File date = DB date ({current_date_str}) - No probing needed")
                                        else:
                                            # Méret vagy dátum eltér: létezik, de módosult - probolni kell
                                            result['output_exists'] = True
                                            result['output_size_matches'] = False
                                            if not output_size_matches and not output_date_matches:
                                                video_loading_log(f"    ✗ NEED OUTPUT PROBE: File size changed ({output_size_current:,} bytes != {saved_file_size:,} bytes) AND File date changed ({current_date_str} != {saved_date_str})")
                                            elif not output_size_matches:
                                                video_loading_log(f"    ✗ NEED OUTPUT PROBE: File size changed ({output_size_current:,} bytes != {saved_file_size:,} bytes)")
                                            else:
                                                video_loading_log(f"    ✗ NEED OUTPUT PROBE: File date changed ({current_date_str} != {saved_date_str})")
                                    else:
                                        result['output_exists'] = False
                                        result['output_size_matches'] = False
                                except (OSError, PermissionError):
                                    # Fájl nem elérhető
                                    result['output_exists'] = False
                                    result['output_size_matches'] = False
                            elif saved_file_size is not None:
                                # Van csak saved fájlméret (nincs dátum): gyors stat() ellenőrzés
                                try:
                                    if result['output_file'] and result['output_file'].exists():
                                        output_stat = result['output_file'].stat()
                                        output_size_current = output_stat.st_size
                                        output_modified_current = output_stat.st_mtime
                                        
                                        # Format date for human-readable output
                                        current_date_str = datetime.fromtimestamp(output_modified_current).strftime('%Y-%m-%d %H:%M:%S')
                                        
                                        video_loading_log(f"  Output file check:")
                                        video_loading_log(f"    File size: {output_size_current:,} bytes | DB size: {saved_file_size:,} bytes")
                                        video_loading_log(f"    File date: {current_date_str} | DB date: (not available)")
                                        
                                        # Ha megegyezik a méret, használjuk a saved adatokat (gyors betöltés)
                                        if saved_file_size == output_size_current:
                                            result['output_exists'] = True
                                            result['output_size_matches'] = True
                                            video_loading_log(f"    ✓ SKIP OUTPUT PROBE: File size = DB size ({output_size_current:,} bytes) - No probing needed (date not checked, no date in DB)")
                                        else:
                                            # Méret eltér: létezik, de módosult
                                            result['output_exists'] = True
                                            result['output_size_matches'] = False
                                            video_loading_log(f"    ✗ NEED OUTPUT PROBE: File size changed ({output_size_current:,} bytes != {saved_file_size:,} bytes)")
                                    else:
                                        result['output_exists'] = False
                                        result['output_size_matches'] = False
                                except (OSError, PermissionError):
                                    result['output_exists'] = False
                                    result['output_size_matches'] = False
                            else:
                                # Nincs saved adat: ellenőrizzük (ritka eset)
                                result['output_exists'] = result['output_file'].exists() if result['output_file'] else False
                                result['output_size_matches'] = False
                        elif saved_status_code_check in ('pending', 'nvenc_queue', 'svt_queue', 'svt_encoding', 'svt_validation', 'svt_crf_search', 'encoding', 'nvenc_encoding'):
                            # Pending/queued státusz esetén ELLENŐRIZZÜK, hogy létezik-e az output fájl
                            # (lehet, hogy az adatbázis frissítés hiányossága miatt nem frissült a státusz)
                            if result['output_file'] and result['output_file'].exists():
                                try:
                                    output_stat = result['output_file'].stat()
                                    output_size_current = output_stat.st_size
                                    output_modified_current = output_stat.st_mtime
                                    result['output_exists'] = True
                                    result['output_size_matches'] = False  # Nincs saved adat, ezért False
                                    video_loading_log(f"  ⚠ Output file exists but status is pending/encoding - will update to completed")
                                except (OSError, PermissionError):
                                    result['output_exists'] = False
                                    result['output_size_matches'] = False
                            else:
                                result['output_exists'] = False
                                result['output_size_matches'] = False
                        else:
                            # Egyéb esetben ellenőrizzük (ritka eset)
                            result['output_exists'] = result['output_file'].exists() if result['output_file'] else False
                            result['output_size_matches'] = False
                    
                    output_cq_crf = output_vmaf = output_psnr = output_frame_count = output_file_size = None
                    output_modified_date = None
                    output_encoder_type = None
                    output_duration_seconds = None
                    should_delete_output = False
                    new_size_bytes = None
                    
                    cq_str = saved_video.get('cq', '-')
                    saved_vmaf = saved_video.get('vmaf', '-')
                    if saved_vmaf != "-":
                        try:
                            vmaf_num = float(saved_vmaf)
                            vmaf_str = format_localized_number(vmaf_num, decimals=1)
                        except (ValueError, TypeError):
                            vmaf_str = saved_vmaf
                    else:
                        vmaf_str = saved_vmaf
                    
                    saved_psnr = saved_video.get('psnr', '-')
                    if saved_psnr != "-":
                        try:
                            psnr_num = float(saved_psnr)
                            psnr_str = format_localized_number(psnr_num, decimals=1)
                        except (ValueError, TypeError):
                            psnr_str = saved_psnr
                    else:
                        psnr_str = saved_psnr
                    
                    progress_str = saved_video.get('progress', '-')
                    new_size_str = saved_video.get('new_size', '-')
                    change_percent_display = saved_video.get('size_change', '-')
                    
                    suspicious_reasons = []
                    # Optimalizálás: ha a saved_video szerint completed és az output létezik, 
                    # csak akkor hívjuk meg a get_output_file_info-t, ha a fájl módosult (méret vagy dátum)
                    saved_status_code = saved_video.get('status_code')
                    if not saved_status_code:
                        saved_status = saved_video.get('status', '')
                        saved_status_code = normalize_status_to_code(saved_status)
                    
                    output_file_modified = None
                    output_file_size_current = None
                    should_probe_output = True
                    
                    # Optimalizálás: ha pending/queued státusz, de az output fájl létezik, probolni kell
                    if saved_status_code in ('pending', 'nvenc_queue', 'svt_queue', 'svt_encoding', 'svt_validation', 'svt_crf_search', 'encoding', 'nvenc_encoding'):
                        # Pending/queued státusz esetén: ha az output fájl létezik, probolni kell (lehet, hogy elkészült)
                        if result.get('output_exists', False):
                            should_probe_output = True  # Probolni kell, hogy frissítsük a státuszt
                            video_loading_log(f"  ⚠ Status is pending/encoding but output file exists - probing to check if completed")
                        else:
                            should_probe_output = False  # Nincs output fájl, valóban pending
                    # Optimalizálás: ha completed státusz és a dátum egyezik, skip-eljük a probe-ot
                    elif result['output_exists'] and saved_status_code in ('completed', 'completed_nvenc', 'completed_svt', 'completed_copy', 'completed_exists'):
                        saved_modified = saved_video.get('output_modified_timestamp')
                        saved_file_size = saved_video.get('output_file_size_bytes')
                        
                        # CSAK akkor skip-eljük a probe-ot, ha MINDKETTŐ (méret ÉS dátum) egyezik (output_size_matches flag)
                        if result.get('output_size_matches', False):
                            # Méret és dátum egyezik: használjuk a saved adatokat (gyors) - NEM olvassuk be a fájlból
                            output_file_modified = saved_modified
                            output_file_size_current = saved_file_size
                            should_probe_output = False  # Skip-eljük a lassú probe-ot és fájlrendszer műveleteket
                        elif saved_modified is not None and saved_file_size is not None:
                            # Van saved adat, de méret vagy dátum eltér: probolni kell (fájl módosult)
                            output_file_modified = saved_modified
                            output_file_size_current = saved_file_size
                            should_probe_output = True  # Probolni kell, mert eltér
                        else:
                            # Ha nincs semmi a JSON-ban, akkor teljes probe (ritka eset)
                            should_probe_output = True
                    
                    if result['output_exists']:
                        if should_probe_output:
                            # Teljes probe - csak akkor hívjuk meg, ha a fájl módosult
                            video_loading_log(f"  PROBING output file (file changed or no saved data)")
                            probe_start = time.time()
                            output_cq_crf, output_vmaf, output_psnr, output_frame_count, output_file_size, output_modified_date, output_encoder_type, should_delete_output, output_duration_seconds = get_output_file_info(result['output_file'])
                            probe_time = (time.time() - probe_start) * 1000
                            video_loading_log(f"  Output probe took {probe_time:.2f}ms: cq={output_cq_crf}, vmaf={output_vmaf}, size={output_file_size}")
                            if output_file_modified is None:
                                try:
                                    output_file_modified = result['output_file'].stat().st_mtime
                                except (OSError, PermissionError):
                                    pass
                            if output_file_size_current is None:
                                try:
                                    output_file_size_current = result['output_file'].stat().st_size
                                except (OSError, PermissionError):
                                    pass
                        else:
                            # Használjuk a saved adatokat (gyors) - minden metaadatot a JSON-ból
                            video_loading_log(f"  ✓ SKIP output probe: using saved data (size and date match)")
                            output_cq_crf = saved_video.get('cq')
                            if output_cq_crf and output_cq_crf != '-':
                                try:
                                    output_cq_crf = int(float(output_cq_crf))
                                except (ValueError, TypeError):
                                    output_cq_crf = None
                            else:
                                output_cq_crf = None
                            output_vmaf = saved_video.get('vmaf')
                            if output_vmaf and output_vmaf != '-':
                                try:
                                    output_vmaf = float(output_vmaf)
                                except (ValueError, TypeError):
                                    output_vmaf = None
                            else:
                                output_vmaf = None
                            output_psnr = saved_video.get('psnr')
                            if output_psnr and output_psnr != '-':
                                try:
                                    output_psnr = float(output_psnr)
                                except (ValueError, TypeError):
                                    output_psnr = None
                            else:
                                output_psnr = None
                            output_frame_count = saved_video.get('source_frame_count')  # Output frame count = source frame count ha completed
                            output_file_size = saved_video.get('output_file_size_bytes') or saved_video.get('new_size_bytes')
                            output_modified_date = saved_video.get('completed_date', '')
                            should_delete_output = False
                            output_duration_seconds = result['source_duration_seconds']
                            
                            # Encoder type a JSON-ból (ha nincs, akkor a status_code-ból)
                            output_encoder_type = saved_video.get('output_encoder_type')
                            if output_encoder_type is None:
                                # Próbáljuk a status_code-ból következtetni
                                if saved_status_code == 'completed_nvenc':
                                    output_encoder_type = 'nvenc'
                                elif saved_status_code == 'completed_svt':
                                    output_encoder_type = 'svt-av1'
                                # Ha még mindig nincs, de completed státusz van, akkor próbáljuk meg a fájlból probolni (gyors, csak Settings tag)
                                elif saved_status_code in ('completed', 'completed_copy', 'completed_exists'):
                                    try:
                                        # Gyors probe csak a Settings tag-ért (nem teljes probe)
                                        probe_cmd = [
                                            FFPROBE_PATH, '-v', 'error',
                                            '-show_entries', 'format_tags=Settings',
                                            '-of', 'default=noprint_wrappers=1:nokey=1',
                                            os.fspath(result['output_file'].absolute())
                                        ]
                                        result_probe = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=5, startupinfo=get_startup_info())
                                        settings_str = result_probe.stdout.strip() if result_probe.stdout else ""
                                        if settings_str:
                                            if 'NVENC' in settings_str.upper() or 'CQ:' in settings_str:
                                                output_encoder_type = 'nvenc'
                                            elif 'SVT-AV1' in settings_str.upper() or 'SVT' in settings_str.upper() or 'CRF:' in settings_str:
                                                output_encoder_type = 'svt-av1'
                                    except Exception:
                                        pass
                        
                        if should_probe_output:
                            if result['source_frame_count'] and output_frame_count and frames_significantly_different(result['source_frame_count'], output_frame_count):
                                suspicious_reasons.append(f"frames: {output_frame_count}/{result['source_frame_count']}")
                        
                        if output_file_size:
                            new_size_bytes = output_file_size
                        else:
                            # Használjuk a saved értéket, ne olvassuk be a fájlból (gyors)
                            new_size_bytes = saved_video.get('output_file_size_bytes') or saved_video.get('new_size_bytes')
                            # Csak akkor olvassuk be, ha nincs a JSON-ban (ritka eset)
                            if new_size_bytes is None:
                                try:
                                    new_size_bytes = result['output_file'].stat().st_size
                                except (OSError, PermissionError):
                                    new_size_bytes = None
                    
                    if not result['output_exists'] and new_size_bytes is None:
                        new_size_bytes = saved_video.get('new_size_bytes')
                    if new_size_bytes is not None:
                        new_size_mb = new_size_bytes / (1024**2)
                        new_size_str = f"{format_localized_number(new_size_mb, decimals=1)} MB"
                        if result['source_size_bytes']:
                            try:
                                change_percent = ((new_size_bytes - result['source_size_bytes']) / result['source_size_bytes']) * 100
                                change_percent_display = f"{format_localized_number(change_percent, decimals=2, show_sign=True)}%"
                            except ZeroDivisionError:
                                change_percent_display = format_localized_number(0, decimals=2) + '%'
                    else:
                        saved_new_size = saved_video.get('new_size', '-')
                        if saved_new_size != "-" and "MB" in saved_new_size:
                            try:
                                size_num = float(saved_new_size.replace("MB", "").strip())
                                new_size_str = f"{format_localized_number(size_num, decimals=1)} MB"
                            except (ValueError, TypeError):
                                new_size_str = saved_new_size
                        else:
                            new_size_str = saved_new_size
                        
                        saved_change = saved_video.get('size_change', '-')
                        if saved_change != "-" and "%" in saved_change:
                            try:
                                has_plus = saved_change.strip().startswith('+')
                                clean_val = saved_change.replace("%", "").replace("+", "").strip()
                                change_num = float(clean_val)
                                change_percent_display = ("+" if has_plus else "") + format_localized_number(change_num, decimals=2, show_sign=False) + "%"
                            except (ValueError, TypeError):
                                change_percent_display = saved_change
                        elif isinstance(change_percent_display, (int, float)):
                            change_percent_display = f"{format_localized_number(change_percent_display, decimals=2, show_sign=True)}%"
                        else:
                            change_percent_display = saved_change
                    
                    size_ratio = None
                    if result['source_size_bytes'] and new_size_bytes:
                        try:
                            size_ratio = new_size_bytes / result['source_size_bytes']
                            if size_ratio < SIZE_MISMATCH_RATIO:
                                suspicious_reasons.append(f"méretarány {size_ratio:.2%}")
                        except ZeroDivisionError:
                            pass
                    
                    duration_ratio = None
                    if result['source_duration_seconds'] and output_duration_seconds:
                        try:
                            duration_ratio = output_duration_seconds / result['source_duration_seconds']
                            if duration_ratio < DURATION_MISMATCH_RATIO:
                                suspicious_reasons.append(f"időtartam {duration_ratio:.2%}")
                        except ZeroDivisionError:
                            pass
                    
                    # Státusz beállítása: először output_encoder_type-ból (DB-ből), majd status_code-ból, végül alapértelmezett
                    saved_output_encoder_type = saved_video.get('output_encoder_type')
                    status_code = saved_video.get('status_code')
                    saved_status_code = status_code  # Elmentjük az eredeti status_code-ot
                    
                    # Ha van output_encoder_type a DB-ben, használjuk azt a státusz beállításához
                    # (nem csak completed státusz esetén, hanem mindig, ha van encoder type)
                    if saved_output_encoder_type:
                        if saved_output_encoder_type == 'nvenc':
                            status_code = 'completed_nvenc'
                        elif saved_output_encoder_type == 'svt-av1':
                            status_code = 'completed_svt'
                        # Ha nincs egyezés, megtartjuk az eredeti status_code-ot (ha van)
                    elif status_code in ('completed', 'completed_nvenc', 'completed_svt', 'completed_copy', 'completed_exists'):
                        # Ha nincs output_encoder_type, de van status_code, próbáljuk abból következtetni
                        if status_code == 'completed_nvenc':
                            saved_output_encoder_type = 'nvenc'
                        elif status_code == 'completed_svt':
                            saved_output_encoder_type = 'svt-av1'
                    
                    if not status_code:
                        saved_status = saved_video.get('status', '')
                        status_code = normalize_status_to_code(saved_status)
                        saved_status_code = status_code  # Frissítjük
                    if not status_code:
                        status_code = 'nvenc_queue' if self.nvenc_enabled.get() else 'svt_queue'
                        saved_status_code = status_code  # Frissítjük
                    status_text = status_code_to_localized(status_code)
                    completed_date = saved_video.get('completed_date', '')
                    
                    duration_str = format_seconds_hms(result['source_duration_seconds']) if result['source_duration_seconds'] else "-"
                    frames_str = str(result['source_frame_count']) if result['source_frame_count'] else "-"
                    
                    # Mindig a frissen képzett relatív útvonalat jelenítsük meg
                    video_name_display = result['video_name'] or ""
                    if not video_name_display:
                        video_name_display = saved_video.get('video_name', str(video_path))
                    # Ha a DB-ben még régi (csak fájlnév) szerepelt, frissítsük az in-memory példányt,
                    # így a mentéskor már az új érték kerül a táblába.
                    if saved_video.get('video_name') != video_name_display:
                        saved_video['video_name'] = video_name_display
                    
                    if result['output_exists'] and not suspicious_reasons and not should_delete_output:
                        if output_cq_crf is not None:
                            cq_str = str(output_cq_crf)
                        if output_vmaf is not None:
                            vmaf_str = format_localized_number(output_vmaf, decimals=1)
                        if output_psnr is not None:
                            psnr_str = format_localized_number(output_psnr, decimals=1)
                        progress_str = '100%'
                        completed_date = output_modified_date or completed_date
                        
                        # Ha van output_encoder_type (fájlból vagy DB-ből), használjuk azt
                        # FONTOS: csak akkor írjuk felül a status_code-ot, ha még nincs encoder-specifikus státusz
                        if status_code not in ('completed_nvenc', 'completed_svt'):
                            if output_encoder_type:
                                if output_encoder_type == 'nvenc':
                                    status_code = 'completed_nvenc'
                                elif output_encoder_type == 'svt-av1':
                                    status_code = 'completed_svt'
                                else:
                                    status_code = 'completed'
                            elif saved_output_encoder_type:
                                # Ha nincs output_encoder_type a fájlból, de van a DB-ben, használjuk azt
                                if saved_output_encoder_type == 'nvenc':
                                    status_code = 'completed_nvenc'
                                elif saved_output_encoder_type == 'svt-av1':
                                    status_code = 'completed_svt'
                                else:
                                    status_code = 'completed'
                            elif status_code not in ('completed', 'completed_nvenc', 'completed_svt', 'completed_copy', 'completed_exists'):
                                # Ha nincs encoder type információ, de completed státusz van, akkor alapértelmezett "completed"
                                status_code = 'completed'
                        
                        status_text = status_code_to_localized(status_code)
                        result['values'] = (video_name_display, status_text, cq_str, vmaf_str, psnr_str, progress_str,
                                          orig_size_str, new_size_str, change_percent_display, duration_str, frames_str, completed_date)
                        result['tag'] = 'completed'
                    else:
                        if result['output_exists']:
                            log_file_check(f"⚠ Hiányos célfájl – {', '.join(suspicious_reasons or ['ismeretlen ok'])}: {video_path.name}")
                            try:
                                result['output_file'].unlink()
                            except (OSError, PermissionError):
                                pass
                            result['output_exists'] = False
                        completed_date = ''
                        status_code = 'nvenc_queue' if self.nvenc_enabled.get() else 'svt_queue'
                        if self.nvenc_enabled.get():
                            status_text = t('status_nvenc_queue')
                        else:
                            status_text = t('status_svt_queue')
                        warning_progress = "⚠ Hiányos célfájl újrakódolása szükséges" if (suspicious_reasons or should_delete_output) else '-'
                        result['values'] = (video_name_display, status_text, "-", "-", "-", warning_progress, orig_size_str, "-", "-", duration_str, frames_str, completed_date)
                        result['tag'] = 'pending'
                    
                    if status_code in ('completed', 'completed_nvenc', 'completed_svt', 'completed_copy', 'completed_exists'):
                        result['tag'] = 'completed'
                    elif status_code in ('svt_queue', 'svt_encoding', 'svt_validation', 'svt_crf_search'):
                        result['tag'] = 'encoding_svt'
                    elif status_code in ('failed', 'source_missing', 'file_missing', 'vmaf_error'):
                        result['tag'] = 'failed'
                    elif status_code in ('needs_check', 'needs_check_nvenc', 'needs_check_svt'):
                        result['tag'] = 'needs_check'
                    else:
                        result['tag'] = 'pending'
                else:
                    # New video, no saved state
                    # Először olvassuk be a fájlméretet (ha még nincs beállítva)
                    if result['source_size_bytes'] is None and video_path.exists():
                        try:
                            stat_info = video_path.stat()
                            result['source_size_bytes'] = stat_info.st_size
                            result['source_modified_timestamp'] = stat_info.st_mtime  # Cache-eléshez
                        except (OSError, PermissionError):
                            result['source_size_bytes'] = None
                    
                    orig_size_mb = None  # Inicializálás
                    if result['source_size_bytes']:
                        orig_size_mb = result['source_size_bytes'] / (1024**2)
                        orig_size_str = f"{format_localized_number(orig_size_mb, decimals=1)} MB"
                    else:
                        orig_size_str = "-"
                    
                    # Probe video
                    video_loading_log(f"  PROBING source video (new video, no saved data)")
                    try:
                        probe_start = time.time()
                        duration, fps = get_video_info(video_path)
                        probe_time = (time.time() - probe_start) * 1000
                        video_loading_log(f"  Source probe took {probe_time:.2f}ms: duration={duration}, fps={fps}")
                        if duration and fps:
                            result['source_frame_count'] = int(duration * fps)
                        result['source_duration_seconds'] = duration
                    except Exception:
                        pass
                    
                    duration_str = format_seconds_hms(result['source_duration_seconds']) if result['source_duration_seconds'] else "-"
                    frames_str = str(result['source_frame_count']) if result['source_frame_count'] else "-"
                    
                    # Output file check
                    if destination_is_empty:
                        result['output_exists'] = False
                    else:
                        result['output_exists'] = result['output_file'].exists()
                    
                    output_cq_crf = None
                    output_vmaf = None
                    output_psnr = None
                    output_frame_count = None
                    output_file_size = None
                    output_modified_date = None
                    output_encoder_type = None
                    should_delete_output = False
                    
                    if result['output_exists']:
                        video_loading_log(f"  PROBING output file (new video)")
                        probe_start = time.time()
                        output_cq_crf, output_vmaf, output_psnr, output_frame_count, output_file_size, output_modified_date, output_encoder_type, should_delete_output, output_duration_seconds = get_output_file_info(result['output_file'])
                        probe_time = (time.time() - probe_start) * 1000
                        video_loading_log(f"  Output probe took {probe_time:.2f}ms: cq={output_cq_crf}, vmaf={output_vmaf}, size={output_file_size}")
                        if result['source_frame_count'] and output_frame_count and frames_significantly_different(result['source_frame_count'], output_frame_count):
                            should_delete_output = True
                            log_file_check(f"⚠ Hiányos célfájl (frames: {output_frame_count}/{result['source_frame_count']}) → újrakódolás: {video_path.name}")
                            try:
                                result['output_file'].unlink()
                            except (OSError, PermissionError):
                                pass
                            result['output_exists'] = False
                    
                    if result['output_exists']:
                        if output_file_size:
                            new_size_mb = output_file_size / (1024**2)
                        else:
                            try:
                                new_size_mb = result['output_file'].stat().st_size / (1024**2)
                            except (OSError, PermissionError):
                                new_size_mb = 0
                        # Ellenőrizzük, hogy orig_size_mb definiálva van-e
                        if orig_size_mb is not None and orig_size_mb > 0:
                            change_percent = ((new_size_mb - orig_size_mb) / orig_size_mb) * 100
                        else:
                            change_percent = 0
                        cq_str = str(output_cq_crf) if output_cq_crf is not None else "-"
                        vmaf_str = format_localized_number(output_vmaf, decimals=1) if output_vmaf is not None else "-"
                        psnr_str = format_localized_number(output_psnr, decimals=1) if output_psnr is not None else "-"
                        progress_str = "100%" if output_cq_crf is not None else "-"
                        if output_encoder_type == 'nvenc':
                            status_str = t('status_completed_nvenc')
                        elif output_encoder_type == 'svt-av1':
                            status_str = t('status_completed_svt')
                        else:
                            status_str = t('status_completed')
                        new_size_str = f"{format_localized_number(new_size_mb, decimals=1)} MB"
                        change_percent_display = f"{format_localized_number(change_percent, decimals=2, show_sign=True)}%"
                        result['values'] = (result['video_name'], status_str, cq_str, vmaf_str, psnr_str, progress_str, orig_size_str, new_size_str, change_percent_display, duration_str, frames_str, output_modified_date or "")
                        result['tag'] = "completed"
                    else:
                        if self.nvenc_enabled.get():
                            status_text = t('status_nvenc_queue')
                        else:
                            status_text = t('status_svt_queue')
                        result['values'] = (result['video_name'], status_text, "-", "-", "-", "-", orig_size_str, "-", "-", duration_str, frames_str, "")
                        result['tag'] = "pending"
                
                # Subtitle files
                valid_subtitles, invalid_subtitles = self._get_validated_subtitles_for_video(video_path)
                result['subtitle_files'] = valid_subtitles
                if invalid_subtitles:
                    result['invalid_subtitles'] = invalid_subtitles
                video_loading_log(f"END process_video_data: {result['video_name']} - SUCCESS")
                
            except Exception as e:
                result['error'] = str(e)
                log_file_check(f"✗ Hiba videó feldolgozása során ({video_path}): {e}")
                video_name_display = result.get('video_name') or str(video_path)
                error_text = str(e)
                if len(error_text) > 120:
                    error_text = error_text[:117] + "..."
                status_text = f"{t('status_load_error')}: {error_text}" if error_text else t('status_load_error')
                load_debug_log(f"process_video_data kivétel: {video_name_display} -> {error_text}")
                video_loading_log(f"END process_video_data: {video_name_display} - ERROR: {error_text}")
                result['values'] = (video_name_display, status_text, "-", "-", "-", "-", "-", "-", "-", "-", "-", "")
                result['tag'] = "failed"
            
            return result
        
        # Párhuzamos feldolgozás - több worker gyorsabb betöltéshez
        total_videos = len(self.video_files)
        # Optimalizált worker szám: nem túl sok, hogy ne legyen használhatatlan a számítógép
        # I/O-bound műveletek, de nem kell túl sok thread (4-8 elég)
        cpu_count = os.cpu_count() or 4
        max_workers = min(8, total_videos, max(4, cpu_count))  # Max 8 worker, min 4 (ha van elég CPU)
        
        # Thread-safe queue az elkészült adatokhoz
        completed_data_queue = queue.Queue()
        processed_count = [0]  # List for mutable counter
        
        # GUI frissítés időzítő (másodpercenként)
        last_update_time = [time.time()]
        
        def update_gui_from_queue(force=False):
            """
            GUI frissítés az elkészült adatokból
            """
            current_time = time.time()
            if not force and current_time - last_update_time[0] < 1.0:  # Csak másodpercenként (kivéve ha force=True)
                if LOAD_DEBUG:
                    load_debug_log(f"update_gui_from_queue kihagyva (throttle) | force={force} | processed={processed_count[0]} | queue={completed_data_queue.qsize()}")
                return
            
            last_update_time[0] = current_time
            items_to_add = []
            if LOAD_DEBUG:
                load_debug_log(f"update_gui_from_queue indul | force={force} | queue={completed_data_queue.qsize()} | processed={processed_count[0]}")
            
            # Összegyűjtjük az elkészült adatokat
            while not completed_data_queue.empty():
                try:
                    data = completed_data_queue.get_nowait()
                    # A 'finished' jelzőt ne számoljuk bele
                    if not data.get('finished'):
                        items_to_add.append(data)
                        processed_count[0] += 1
                except queue.Empty:
                    break
            
            if LOAD_DEBUG:
                load_debug_log(f"update_gui_from_queue: {len(items_to_add)} elem került feldolgozásra, számláló={processed_count[0]}/{total_videos}")
            
            # Hozzáadjuk a táblázathoz
            for data in items_to_add:
                # Ellenőrizzük a 'finished' jelzőt
                if data.get('finished'):
                    continue
                
                # Biztonsági ellenőrzés: values és tag kell legyen
                if data.get('values') is None or data.get('tag') is None:
                    log_file_check(f"⚠ Hiányzó values vagy tag: {data.get('video_path', 'unknown')}")
                    continue
                
                try:
                    order_num = data.get('order_num', 0)
                    values = data['values']
                    tag = data['tag']
                    
                    # Ellenőrizzük, hogy még nincs-e már a táblázatban (duplikáció elkerülése)
                    if data.get('video_path') and data['video_path'] in self.video_items:
                        continue
                    
                    item_id = self.tree.insert("", tk.END, text=str(order_num), values=values, tags=(tag,))
                    if data.get('video_path'):
                        self.video_items[data['video_path']] = item_id
                    if data.get('output_file'):
                        self.video_to_output[data['video_path']] = data['output_file']
                    
                    # Tree item mögötti eredeti adatok tárolása (gyors DB mentéshez, parse-olás nélkül)
                    original_data = {}
                    if data.get('source_duration_seconds') is not None:
                        original_data['source_duration_seconds'] = data['source_duration_seconds']
                    if data.get('source_frame_count') is not None:
                        original_data['source_frame_count'] = data['source_frame_count']
                    if data.get('source_fps') is not None:
                        original_data['source_fps'] = data['source_fps']
                    if data.get('output_encoder_type'):
                        original_data['output_encoder_type'] = data['output_encoder_type']
                    if original_data:  # Csak akkor tároljuk, ha van adat
                        self.tree_item_data[item_id] = original_data
                    
                    # Cache-eljük a stat() értékeket (hidegindítás optimalizáláshoz)
                    video_path = data.get('video_path')
                    if video_path and data.get('source_size_bytes') is not None:
                        # Cache-eljük a source_size_bytes-t (betöltéskor stat()-oltunk)
                        cache_entry = {'source_size_bytes': data['source_size_bytes']}
                        # Ha van source_modified_timestamp a result-ban, azt is cache-eljük
                        # Megjegyzés: a process_video_data-ban a stat() eredménye source_modified_current-ben van
                        # De nem mindig kerül be a result-ba, mert csak melegindításnál használjuk
                        # Hidegindításnál új videónál is stat()-olunk, de a timestamp-et nem mindig mentjük
                        # Próbáljuk meg a result-ból kiolvasni, ha van
                        if 'source_modified_timestamp' in data and data['source_modified_timestamp'] is not None:
                            cache_entry['source_modified_timestamp'] = data['source_modified_timestamp']
                        self.video_stat_cache[video_path] = cache_entry
                    
                    # Completed státusz ellenőrzés
                    if is_status_completed(values[self.COLUMN_INDEX['status']] if len(values) > self.COLUMN_INDEX['status'] else "") and self.hide_completed.get():
                        try:
                            self.tree.detach(item_id)
                            self.hidden_items.add(item_id)
                        except (tk.TclError, KeyError, AttributeError):
                            pass
                    
                    # Subtitle files
                    for sub_path, lang_part in data.get('subtitle_files', []):
                        iso_code = normalize_language_code(lang_part)
                        lang_display = f"{lang_part if lang_part else 'UND'} ({iso_code})"
                        sub_item_id = self.tree.insert(item_id, tk.END, text="", values=(lang_display, "", "", "", "", "", "", "", "", "", "", "", ""), tags=("subtitle",))
                        self.subtitle_items[sub_item_id] = (sub_path, lang_part)
                except (tk.TclError, KeyError, AttributeError, TypeError, ValueError) as e:
                    log_file_check(f"⚠ Hiba GUI frissítés során: {e}")
                    continue
            
            # Státusz frissítés
            if items_to_add:
                self.status_label.config(text=f"Videók feldolgozása: {processed_count[0]}/{total_videos}")
                # Rendezzük az elemeket order_num szerint (ABC sorrend)
                self._sort_tree_by_order_num()
                # Nem hívjuk meg self.root.update()-et, mert az időzítő kezeli a frissítést
        
        # Párhuzamos feldolgozás indítása
        self.status_label.config(text=f"Videók feldolgozása: 0/{total_videos}")
        self.root.update()
        
        # Thread pool és futures
        executor = ThreadPoolExecutor(max_workers=max_workers)
        future_to_video = {executor.submit(process_video_data, vp): vp for vp in self.video_files}
        all_futures = list(future_to_video.keys())
        
        # Külön thread a futures befejezésének kezelésére (nem blokkolja a főszálat)
        def collect_results():
            """Külön thread-ben gyűjti az eredményeket"""
            try:
                for future in as_completed(all_futures):
                    try:
                        data = future.result()
                        completed_data_queue.put(data)
                        video_path = future_to_video.get(future)
                        load_debug_log(f"Collector: kész {video_path} | error={bool(data.get('error')) if isinstance(data, dict) else 'n/a'}")
                    except Exception as e:
                        log_file_check(f"✗ Hiba thread eredményének lekérdezésekor: {e}")
                        video_path = future_to_video.get(future)
                        error_data = build_load_error_result(video_path, e)
                        completed_data_queue.put(error_data)
            except Exception as e:
                log_file_check(f"✗ Hiba as_completed ciklusban: {e}")
            finally:
                # Jelezzük, hogy minden kész
                load_debug_log("Collector: minden future feldolgozva, FINISHED jelzés küldése")
                completed_data_queue.put({'finished': True})
        
        # Indítjuk a collector thread-et
        collector_thread = threading.Thread(target=collect_results, daemon=True)
        collector_thread.start()
        
        # Flag a befejezés jelzésére
        final_update_called = [False]
        final_update_running = [False]
        
        # GUI frissítés időzítő (nem blokkolja a főszálat)
        def periodic_gui_update():
            """Időzítővel hívott GUI frissítés"""
            # Ha már meghívtuk a final_gui_update-et, ne csináljunk semmit
            if final_update_called[0]:
                if LOAD_DEBUG:
                    load_debug_log("periodic_gui_update: final_update már meghívva, kilép")
                return
            
            update_gui_from_queue(force=False)
            
            # Ellenőrizzük, hogy minden kész van-e
            if collector_thread.is_alive():
                # Még fut, folytatjuk az időzítőt (100ms-enként, hogy ne blokkolja)
                if LOAD_DEBUG:
                    load_debug_log(f"periodic_gui_update: collector még fut | processed={processed_count[0]}/{total_videos} | queue={completed_data_queue.qsize()}")
                self.root.after(100, periodic_gui_update)
            else:
                # Befejeződött, utolsó frissítés
                # Várunk egy kicsit, hogy a collector thread befejezze az utolsó adatokat
                if not completed_data_queue.empty():
                    if LOAD_DEBUG:
                        load_debug_log(f"periodic_gui_update: collector kész, de queue nem üres (size={completed_data_queue.qsize()}), újraellenőrzés 50ms múlva")
                    self.root.after(50, periodic_gui_update)
                elif not final_update_called[0]:  # Csak akkor, ha még nem hívtuk meg
                    final_update_called[0] = True  # Jelezzük, hogy meghívjuk
                    if LOAD_DEBUG:
                        load_debug_log(f"periodic_gui_update: collector kész, final_gui_update ütemezése | processed={processed_count[0]}/{total_videos} | queue={completed_data_queue.qsize()}")
                    self.root.after(200, final_gui_update)
        
        def final_gui_update():
            """Utolsó GUI frissítés - minden adat hozzáadása"""
            # Ha már fut, ne csináljunk semmit (race condition elkerülése)
            if final_update_running[0]:
                if LOAD_DEBUG:
                    load_debug_log("final_gui_update: már fut, kihagyjuk")
                return
            # Ha idő előtt hívnánk meg (még van elem a queue-ban), várjunk
            if not completed_data_queue.empty() and processed_count[0] < total_videos:
                if LOAD_DEBUG:
                    load_debug_log(f"final_gui_update: queue még nem üres (size={completed_data_queue.qsize()}), újraütemezés")
                self.root.after(50, final_gui_update)
                return

            final_update_running[0] = True  # Jelezzük, hogy fut
            if LOAD_DEBUG:
                load_debug_log(f"final_gui_update indul | processed={processed_count[0]}/{total_videos} | queue={completed_data_queue.qsize()}")
            
            try:
                # Helper függvény a maradék adatok feldolgozásához
                def process_remaining_data():
                    try:
                        # Utolsó GUI frissítés - minden maradék adat (időellenőrzés nélkül, force=True)
                        load_debug_log("process_remaining_data: force frissítés meghívása") if LOAD_DEBUG else None
                        update_gui_from_queue(force=True)
                        
                        # Még egyszer ellenőrizzük, hogy nincs-e maradék adat
                        items_to_add = []
                        while not completed_data_queue.empty():
                            try:
                                data = completed_data_queue.get_nowait()
                                if data.get('finished'):
                                    continue
                                items_to_add.append(data)
                                processed_count[0] += 1
                            except queue.Empty:
                                break
                        
                        # Feldolgozzuk az adatokat
                        if items_to_add:
                            if LOAD_DEBUG:
                                load_debug_log(f"process_remaining_data: extra {len(items_to_add)} elem feldolgozása, számláló={processed_count[0]}/{total_videos}")
                            for data in items_to_add:
                                # Ellenőrizzük a 'finished' jelzőt
                                if data.get('finished'):
                                    continue
                                
                                # Hiba ellenőrzés
                                if data.get('error'):
                                    continue
                                
                                # Biztonsági ellenőrzés: values és tag kell legyen
                                if data.get('values') is None or data.get('tag') is None:
                                    log_file_check(f"⚠ Hiányzó values vagy tag: {data.get('video_path', 'unknown')}")
                                    continue
                                
                                try:
                                    order_num = data.get('order_num', 0)
                                    values = data['values']
                                    tag = data['tag']
                                    
                                    # Ellenőrizzük, hogy még nincs-e már a táblázatban (duplikáció elkerülése)
                                    if data.get('video_path') and data['video_path'] in self.video_items:
                                        continue
                                    
                                    item_id = self.tree.insert("", tk.END, text=str(order_num), values=values, tags=(tag,))
                                    if data.get('video_path'):
                                        self.video_items[data['video_path']] = item_id
                                    if data.get('output_file'):
                                        self.video_to_output[data['video_path']] = data['output_file']
                                    
                                    # Completed státusz ellenőrzés
                                    if is_status_completed(values[self.COLUMN_INDEX['status']] if len(values) > self.COLUMN_INDEX['status'] else "") and self.hide_completed.get():
                                        try:
                                            self.tree.detach(item_id)
                                            self.hidden_items.add(item_id)
                                        except (tk.TclError, KeyError, AttributeError):
                                            pass
                                    
                                    # Subtitle files
                                    for sub_path, lang_part in data.get('subtitle_files', []):
                                        iso_code = normalize_language_code(lang_part)
                                        lang_display = f"{lang_part if lang_part else 'UND'} ({iso_code})"
                                        sub_item_id = self.tree.insert(item_id, tk.END, text="", values=(lang_display, "", "", "", "", "", "", "", "", "", "", "", ""), tags=("subtitle",))
                                        self.subtitle_items[sub_item_id] = (sub_path, lang_part)
                                except (tk.TclError, KeyError, AttributeError, TypeError, ValueError) as e:
                                    log_file_check(f"⚠ Hiba GUI frissítés során: {e}")
                                    continue
                            
                            # Utolsó állapot frissítése – biztosan elérjük a total_videos értéket
                            self.status_label.config(text=f"Videók feldolgozása: {processed_count[0]}/{total_videos}")
                        
                        # Befejező műveletek
                        finish_loading()
                    except Exception as e:
                        log_file_check(f"✗ Hiba process_remaining_data során: {e}")
                        import traceback
                        log_file_check(traceback.format_exc())
                        finish_loading()
                
                def finish_loading():
                    """Befejezi a betöltést"""
                    try:
                        # Biztosítjuk, hogy a source_path és dest_path be legyen állítva
                        if not hasattr(self, 'source_path') or not self.source_path:
                            source = self.source_entry.get()
                            if source:
                                self.source_path = Path(source)
                        if not hasattr(self, 'dest_path') or not self.dest_path:
                            dest = self.dest_entry.get()
                            if dest:
                                self.dest_path = Path(dest)
                        
                        if LOAD_DEBUG:
                            load_debug_log(f"finish_loading: processed={processed_count[0]}/{total_videos} | videók a fában={len(self.video_items)} | queue={completed_data_queue.qsize()}")
                            if processed_count[0] < total_videos:
                                missing_videos = []
                                try:
                                    for video_path in self.video_files:
                                        if video_path not in self.video_items:
                                            missing_videos.append(str(video_path))
                                            if len(missing_videos) >= 10:
                                                break
                                except Exception as debug_exc:
                                    missing_videos.append(f"<hiba a hiányzó lista készítésekor: {debug_exc}>")
                                load_debug_log(f"finish_loading: hiányzó elemek becslése ({total_videos - processed_count[0]} db). Példa: {missing_videos}")
                            if getattr(self, 'last_load_errors', None):
                                err_count = len(self.last_load_errors)
                                if err_count:
                                    load_debug_log(f"Betöltési hibák összesen: {err_count}")
                                    for video_name, status_text in self.last_load_errors[:20]:
                                        load_debug_log(f"  - {video_name}: {status_text}")
                                    if err_count > 20:
                                        load_debug_log(f"  ... +{err_count - 20} további hiba")
                        # Executor lezárása
                        try:
                            executor.shutdown(wait=False)
                        except Exception as e:
                            log_file_check(f"⚠ Hiba executor lezárása során: {e}")
                        
                        # Utolsó rendezés order_num szerint (ABC sorrend)
                        self._sort_tree_by_order_num()
                        
                        # Befejező műveletek
                        self.update_summary_row()
                        self.status_label.config(text=f"Betöltve: {len(self.video_files)} videó")
                        self.is_loading_videos = False
                        
                        # Debug: ellenőrizzük a video_items állapotát
                        if LOAD_DEBUG:
                            pending_count = 0
                            completed_count = 0
                            for video_path, item_id in self.video_items.items():
                                try:
                                    tags = self.tree.item(item_id, 'tags') or ()
                                    current_values = self.tree.item(item_id, 'values')
                                    status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                                    if any(tag in ('pending', 'encoding_nvenc', 'encoding_svt', 'needs_check', 'needs_check_nvenc', 'needs_check_svt') for tag in tags):
                                        pending_count += 1
                                    elif any(tag == 'completed' for tag in tags):
                                        completed_count += 1
                                except Exception:
                                    pass
                            load_debug_log(f"finish_loading: video_items={len(self.video_items)} | pending={pending_count} | completed={completed_count}")
                        
                        # Biztosan engedélyezzük a Start gombot betöltés után, ha vannak betöltött videók
                        # és nincs encoding folyamatban
                        if getattr(self, 'start_button', None) and not self.is_encoding:
                            if self.video_items or self.video_files:
                                # Ha vannak videók, aktiváljuk a gombot (a has_pending_tasks() már ellenőrizte)
                                has_pending = self.has_pending_tasks()
                                if LOAD_DEBUG:
                                    load_debug_log(f"finish_loading: start_button aktiválás | has_pending={has_pending} | video_items={len(self.video_items)} | video_files={len(self.video_files)}")
                                if has_pending or self.video_items:
                                    self.start_button.config(text=t('btn_start'), command=self.start_encoding, state=tk.NORMAL)
                                    if LOAD_DEBUG:
                                        load_debug_log(f"finish_loading: start_button aktiválva (state={self.start_button.cget('state')})")
                        
                        # Frissítjük a start gomb állapotát (ez megerősíti az állapotot, nem írja felül)
                        self.update_start_button_state()

                        # Ha a Start gomb megnyomása közben automatikus betöltést kértünk, most indulhat a kódolás
                        if getattr(self, 'auto_start_after_load', False):
                            has_items = bool(self.video_items)
                            has_tasks = self.has_pending_tasks() if has_items else False
                            if has_items and has_tasks:
                                if LOAD_DEBUG:
                                    load_debug_log("finish_loading: auto_start_after_load -> start_encoding ütemezése")
                                self.auto_start_after_load = False

                                def _delayed_start():
                                    # Csak akkor induljunk, ha időközben nem lett újra betöltés vagy kódolás
                                    if not self.is_encoding and not self.is_loading_videos:
                                        self.start_encoding()

                                self.root.after(150, _delayed_start)
                            else:
                                if LOAD_DEBUG:
                                    load_debug_log("finish_loading: auto_start flag törölve (nincs betöltött feladat)")
                                self.auto_start_after_load = False
                        if getattr(self, 'immediate_stop_button', None):
                            self.immediate_stop_button.config(state=tk.DISABLED)
                        self.progress_bar['maximum'] = len(self.video_files)
                        self.progress_bar['value'] = 0
                        log_file_check(f"Sikeresen betöltve: {len(self.video_files)} videó.")
                        
                        # Check and fix misnamed .av1.mkv copies after loading
                        try:
                            fixed_count = self.check_and_fix_misnamed_copies()
                            if fixed_count > 0:
                                log_file_check(f"✓ {fixed_count} hibásan elnevezett másolat javítva")
                                self.update_summary_row()  # Refresh summary to show corrected files
                        except Exception as e:
                            log_file_check(f"⚠ Hiba misnamed copy ellenőrzés során: {e}")
                        
                        # Adatbázis mentés háttérszálban, hogy ne blokkolja a GUI frissítést
                        # Biztosítjuk, hogy a source_path és dest_path be legyen állítva
                        if hasattr(self, 'source_path') and hasattr(self, 'dest_path'):
                            def save_db_in_background():
                                thread_start_time = time.time()
                                try:
                                    if LOAD_DEBUG:
                                        load_debug_log(f"Adatbázis mentés háttérszálban indul | source={self.source_path} | dest={self.dest_path} | db_path={self.db_path}")
                                    if LOG_WRITER:
                                        try:
                                            LOG_WRITER.write(f"[save_db_in_background] Adatbázis mentés háttérszálban indul | source={self.source_path} | dest={self.dest_path} | db_path={self.db_path}\n")
                                            LOG_WRITER.flush()
                                        except Exception:
                                            pass
                                    # Üzenet küldése, hogy az adatbázis mentés elkezdődött
                                    if hasattr(self, 'encoding_queue'):
                                        self.encoding_queue.put(("copy_status", f"Adatbázis állapot mentése: {self.db_path.name}"))
                                    
                                    # Progress callback a hidegindítás utáni DB mentéshez
                                    def progress_callback(msg):
                                        """Progress callback az adatbázis mentéshez"""
                                        try:
                                            if hasattr(self, 'encoding_queue'):
                                                self.encoding_queue.put_nowait(("db_progress", msg))
                                        except queue.Full:
                                            pass
                                    
                                    self.save_state_to_db(progress_callback=progress_callback)
                                    thread_duration = time.time() - thread_start_time
                                    # Ellenőrizzük, hogy az adatbázis tényleg létrejött-e
                                    if self.db_path.exists():
                                        file_size = self.db_path.stat().st_size
                                        if LOAD_DEBUG:
                                            load_debug_log(f"✓ Adatbázis mentés háttérszálban befejezve | fájl: {self.db_path} | méret: {file_size} bájt | időtartam: {thread_duration:.2f}s")
                                        if LOG_WRITER:
                                            try:
                                                LOG_WRITER.write(f"✓ [save_db_in_background] SQLite állapot sikeresen mentve: {self.db_path} ({file_size} bájt) | időtartam: {thread_duration:.2f}s\n")
                                                LOG_WRITER.flush()
                                            except Exception:
                                                pass
                                        # Üzenet küldése, hogy az adatbázis mentés sikeresen befejeződött
                                        if hasattr(self, 'encoding_queue'):
                                            self.encoding_queue.put(("copy_status", f"✓ Adatbázis állapot sikeresen mentve: {self.db_path.name} ({file_size} bájt)"))
                                            # Hidegindítás utáni automatikus mentés - nem jelenítjük meg notification-t
                                        # Flag beállítása: betöltés utáni DB mentés befejeződött
                                        if hasattr(self, 'load_db_save_completed'):
                                            self.load_db_save_completed.set()
                                    else:
                                        error_msg = f"⚠ Adatbázis fájl nem jött létre: {self.db_path}"
                                        if LOAD_DEBUG:
                                            load_debug_log(error_msg)
                                        if LOG_WRITER:
                                            try:
                                                LOG_WRITER.write(f"⚠ [save_db_in_background] {error_msg} | időtartam: {thread_duration:.2f}s\n")
                                                LOG_WRITER.flush()
                                            except Exception:
                                                pass
                                        if hasattr(self, 'encoding_queue'):
                                            self.encoding_queue.put(("copy_status", f"⚠ Adatbázis fájl nem jött létre"))
                                            # Hidegindítás utáni automatikus mentés hiba - nem jelenítjük meg notification-t
                                except Exception as e:
                                    thread_duration = time.time() - thread_start_time
                                    error_msg = f"✗ Hiba adatbázis mentés során (háttérszál): {e}"
                                    if LOAD_DEBUG:
                                        load_debug_log(error_msg)
                                    if LOG_WRITER:
                                        try:
                                            LOG_WRITER.write(f"✗ [save_db_in_background] {error_msg} | időtartam: {thread_duration:.2f}s\n")
                                            import traceback
                                            LOG_WRITER.write(traceback.format_exc())
                                            LOG_WRITER.flush()
                                        except Exception:
                                            pass
                                    # Hiba esetén is üzenet küldése
                                    if hasattr(self, 'encoding_queue'):
                                        self.encoding_queue.put(("copy_status", f"✗ Adatbázis mentés hiba: {e}"))
                                        # Hidegindítás utáni automatikus mentés hiba - nem jelenítjük meg notification-t
                                finally:
                                    # Logoljuk, hogy a thread befejeződött (mindenképpen)
                                    thread_duration = time.time() - thread_start_time
                                    if LOG_WRITER:
                                        try:
                                            LOG_WRITER.write(f"[save_db_in_background] Háttérszál befejeződött | időtartam: {thread_duration:.2f}s\n")
                                            LOG_WRITER.flush()
                                        except Exception:
                                            pass
                            
                            # Indítjuk a háttérszálat
                            db_thread = self._start_db_thread(save_db_in_background, name="SaveDBBackground")
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"[load_videos] DB mentés háttérszál elindítva (daemon=True)\n")
                                    LOG_WRITER.flush()
                                except Exception:
                                    pass
                            if LOAD_DEBUG:
                                load_debug_log(f"DB mentés háttérszál elindítva (daemon=True)")
                        else:
                            if LOAD_DEBUG:
                                load_debug_log(f"Adatbázis mentés kihagyva: source_path vagy dest_path nincs beállítva")
                    except Exception as e:
                        log_file_check(f"⚠ Hiba befejező műveletek során: {e}")
                
                # Meghívjuk a process_remaining_data() függvényt, hogy feldolgozza a maradék adatokat és meghívja a finish_loading()-ot
                process_remaining_data()
            except Exception as e:
                log_file_check(f"✗ Kritikus hiba final_gui_update során: {e}")
                import traceback
                log_file_check(traceback.format_exc())
                # Hiba esetén is próbáljuk meg befejezni a betöltést
                try:
                    finish_loading()
                except Exception:
                    pass
        
        # Indítjuk az időzítőt
        self.root.after(100, periodic_gui_update)
        
        # Régi szekvenciális ciklus eltávolítva - a fenti párhuzamos feldolgozás helyettesíti
        # Most már nincs szükség a régi for ciklusra, mert minden adat párhuzamosan készül el
        # A befejező műveletek a final_gui_update függvényben történnek
    
    def show_debug_dialog(self, current_step, next_step, file_info, continue_event):
        """Debug dialog"""
        dialog = tk.Toplevel(self.root)
        dialog.title("🛑 Debug Mód")
        dialog.transient(self.root)
        dialog.grab_set()
        
        canvas = tk.Canvas(dialog, width=600, height=400)
        scrollbar = ttk.Scrollbar(dialog, orient=tk.VERTICAL, command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        content_frame = ttk.Frame(scrollable_frame, padding="20")
        content_frame.pack(fill=tk.BOTH, expand=True)
        
        ttk.Label(content_frame, text="🛑 DEBUG MEGÁLLÁS", font=("Arial", 16, "bold")).pack(pady=10)
        
        ttk.Label(content_frame, text="Jelenlegi:", font=("Arial", 10, "bold")).pack(anchor=tk.W)
        ttk.Label(content_frame, text=current_step, wraplength=550, foreground="blue").pack(anchor=tk.W, pady=5)
        
        ttk.Label(content_frame, text="Következő:", font=("Arial", 10, "bold")).pack(anchor=tk.W, pady=(10,0))
        ttk.Label(content_frame, text=next_step, wraplength=550, foreground="green").pack(anchor=tk.W, pady=5)
        
        if file_info:
            ttk.Label(content_frame, text="Info:", font=("Arial", 9, "italic")).pack(anchor=tk.W, pady=(10,0))
            ttk.Label(content_frame, text=file_info, wraplength=550, foreground="gray").pack(anchor=tk.W, pady=5)
        
        button_frame = ttk.Frame(dialog, padding="10")
        button_frame.pack(side=tk.BOTTOM, fill=tk.X)
        
        def on_continue():
            dialog.destroy()
            continue_event.set()
        
        ttk.Button(button_frame, text="▶ Tovább", command=on_continue, width=20).pack()
        
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        dialog.update_idletasks()
        required_height = min(scrollable_frame.winfo_reqheight() + 100, 600)
        dialog.geometry(f"620x{required_height}")
        
        x = (dialog.winfo_screenwidth() // 2) - (620 // 2)
        y = (dialog.winfo_screenheight() // 2) - (required_height // 2)
        dialog.geometry(f"+{x}+{y}")
        
    def start_encoding(self):
        """Start the encoding process.
        
        Initiates the encoding workflow:
        1. Copies non-video files.
        2. Saves current state to database.
        3. Starts worker threads (NVENC, SVT-AV1).
        """
        # Ha még tart a betöltés, soroljuk be automatikus indulásra
        if self.is_loading_videos:
            self.auto_start_after_load = True
            return

        # Ha nincs semmilyen videó a táblázatban, de a forrás/cél mappa ki van töltve,
        # automatikusan töltsük be a listát, majd induljon a kódolás.
        if not self.video_items:
            source_path_str = self.source_entry.get()
            dest_path_str = self.dest_entry.get()
            if source_path_str and os.path.exists(source_path_str) and dest_path_str:
                self.auto_start_after_load = True
                self.load_videos()
                return

        if not self.video_files:
            messagebox.showwarning("Figyelem", "Nincs videó!")
            return
        
        # Check and fix misnamed .av1.mkv copies before encoding starts
        try:
            fixed_count = self.check_and_fix_misnamed_copies()
            if fixed_count > 0 and LOG_WRITER:
                try:
                    LOG_WRITER.write(f"\n✓ {fixed_count} misnamed copy fixed\n")
                    LOG_WRITER.flush()
                except (OSError, IOError):
                    pass
        except Exception as e:
            if LOG_WRITER:
                try:
                    LOG_WRITER.write(f"\n⚠ Error checking misnamed copies: {e}\n")
                    LOG_WRITER.flush()
                except (OSError, IOError):
                    pass
        
        STOP_EVENT.clear()
        self.graceful_stop_requested = False

        # Korábbi SVT-AV1 feladatok törlése (újratervezéskor újra soroljuk őket)
        try:
            while not SVT_QUEUE.empty():
                SVT_QUEUE.get_nowait()
                SVT_QUEUE.task_done()
        except queue.Empty:
            pass

        self.current_min_vmaf = float(self.min_vmaf.get())
        self.current_vmaf_step = float(self.vmaf_step.get())
        self.current_max_encoded_percent = int(self.max_encoded_percent.get())

        global DEBUG_MODE
        DEBUG_MODE = self.debug_mode.get()
        
        # SVT-AV1 queue-ban lévő videók betöltése a SVT_QUEUE-ba
        initial_min_vmaf = float(self.min_vmaf.get())
        vmaf_step = float(self.vmaf_step.get())
        max_encoded = int(self.max_encoded_percent.get())
        resize_enabled = self.resize_enabled.get()
        resize_height = self.resize_height.get()
        
        for video_path in self.video_files:
            if video_path not in self.video_items:
                continue
            
            item_id = self.video_items[video_path]
            current_values = self.tree.item(item_id, 'values')
            current_status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
            status_code = normalize_status_to_code(current_status)
            
            # Ha SVT-AV1 queue-ban van, betöltjük a SVT_QUEUE-ba
            if status_code == 'svt_queue':
                output_file = get_output_filename(video_path, self.source_path, self.dest_path)
                valid_subtitles, invalid_subtitles = self._get_validated_subtitles_for_video(video_path)
                subtitle_files = valid_subtitles
                orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
                
                svt_task = {
                    'video_path': video_path,
                    'output_file': output_file,
                    'subtitle_files': subtitle_files,
                    'invalid_subtitles': invalid_subtitles,
                    'item_id': item_id,
                    'orig_size_str': orig_size_str,
                    'initial_min_vmaf': initial_min_vmaf,
                    'vmaf_step': vmaf_step,
                    'max_encoded': max_encoded,
                    'resize_enabled': resize_enabled,
                    'resize_height': resize_height,
                    'audio_compression_enabled': self.audio_compression_enabled.get(),
                    'audio_compression_method': self.audio_compression_method.get(),
                    'reason': 'start_encoding'
                }
                # Graceful stop ellenőrzése - ne indítsunk új feladatot, ha leállítás kérvényezve van
                if self.graceful_stop_requested:
                    # Ne indítsunk új feladatot, ha graceful stop kérvényezve van
                    continue
                
                SVT_QUEUE.put(svt_task)
                completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                self.encoding_queue.put(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                self.encoding_queue.put(("tag", item_id, "encoding_svt"))
            
            # Ha VMAF / PSNR ellenőrzésre vár, betöltjük a VMAF_QUEUE-ba
            elif status_code in ('vmaf_waiting', 'psnr_waiting', 'vmaf_psnr_waiting'):
                output_file = self.video_to_output.get(video_path)
                if output_file and output_file.exists():
                    orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
                    vmaf_task = {
                        'video_path': video_path,
                        'output_file': output_file,
                        'item_id': item_id,
                        'orig_size_str': orig_size_str,
                        'check_vmaf': status_code != 'psnr_waiting',
                        'check_psnr': status_code != 'vmaf_waiting'
                    }
                    VMAF_QUEUE.put(vmaf_task)
                    # Státusz marad a mentett várakozási értéken
        
        # Start gomb "Leállítás" gombként működik futás közben
        self.start_button.config(text=t('btn_stop'), command=self.stop_encoding_graceful, state=tk.NORMAL)
        self.immediate_stop_button.config(state=tk.NORMAL)
        # Videók betöltése gomb inaktívvá tétele
        self.load_videos_btn.config(state=tk.DISABLED)
        
        # 1. LÉPÉS: Nem-videó fájlok másolása (ha van cél mappa)
        if self.dest_path:
            def copy_callback(msg):
                try:
                    # Tuple formátum: (típus, total, current, message)
                    if isinstance(msg, tuple):
                        self.encoding_queue.put_nowait(("copy_progress", msg))
                    else:
                        # Régi formátum kompatibilitás
                        self.encoding_queue.put_nowait(("copy_status", msg))
                except queue.Full:
                    pass
            
            def copy_files_sync():
                """Szinkron másolás - a Start gomb után azonnal"""
                try:
                    copy_non_video_files(self.source_path, self.dest_path, copy_callback)
                except Exception as e:
                    try:
                        copy_callback(("copy_error", f"✗ Hiba nem-videó fájlok másolásakor: {e}"))
                    except Exception:
                        pass
            
            # Másolás aszinkron módon (hogy lássuk a progressbar-t és ne fagyjon be)
            copy_thread = threading.Thread(target=copy_files_sync, daemon=True)
            copy_thread.start()
            self.copy_thread = copy_thread
            
            # Várunk a másolás befejezésére, de közben frissítjük a GUI-t
            while copy_thread.is_alive():
                self.root.update()  # GUI frissítés
                time.sleep(0.1)  # 100ms várakozás
        
        # 2. LÉPÉS: Adatbázis mentés (aszinkron módon, hogy ne blokkolja a GUI-t)
        # Notification-ban jelenítjük meg, ne a status_label-ban
        self.root.after(0, lambda: self.db_notification_label.config(text="Adatbázis mentése...", foreground="blue"))
        self.root.update()  # GUI frissítés
        
        # Ellenőrizzük, hogy már fut-e DB mentés (hidegindítás után)
        # Ha fut, várunk rá, hogy ne legyen lock ütközés
        db_save_in_progress = False
        # Ellenőrizzük, hogy a betöltés utáni DB mentés befejeződött-e
        load_db_save_done = False
        if hasattr(self, 'load_db_save_completed'):
            load_db_save_done = self.load_db_save_completed.is_set()
        
        try:
            # Próbáljuk meg lockolni a DB-t (non-blocking)
            if self.db_lock.acquire(blocking=False):
                # Nincs futó DB mentés, szabadon használhatjuk
                self.db_lock.release()
            else:
                # Már fut DB mentés, várunk rá
                db_save_in_progress = True
                self.root.after(0, lambda: self.db_notification_label.config(text="Adatbázis mentés folyamatban... várás...", foreground="orange"))
                self.root.update()
        except Exception:
            pass
        
        db_saved = threading.Event()
        db_error = [None]
        
        def save_db_async():
            """Adatbázis mentés aszinkron módon"""
            try:
                # Ha már fut DB mentés, várunk rá
                if db_save_in_progress:
                    # Várunk maximum 5 percet a korábbi DB mentés befejezésére
                    wait_start = time.time()
                    wait_timeout = 300  # 5 perc
                    while not self.db_lock.acquire(blocking=False):
                        if time.time() - wait_start > wait_timeout:
                            db_error[0] = Exception("Adatbázis mentés timeout: korábbi mentés nem fejeződött be 5 perc alatt")
                            db_saved.set()
                            return
                        time.sleep(0.5)
                    self.db_lock.release()
                
                # Ha a betöltés utáni DB mentés már befejeződött, akkor a Start gomb után
                # csak akkor mentünk újra, ha tényleg változott valami (pl. queue-k feltöltésekor státusz változás)
                # De mivel a queue-k feltöltésekor változhatnak a státuszok, mindig mentünk
                # (ez normális viselkedés, mert a Start gomb után a státuszok frissülnek)
                
                def progress_callback(msg):
                    """Progress callback az adatbázis mentéshez"""
                    try:
                        self.encoding_queue.put_nowait(("db_progress", msg))
                    except queue.Full:
                        pass
                
                self.save_state_to_db(progress_callback=progress_callback)
                # Notification-t a db_progress üzenetek automatikusan megjelenítik, nem kell külön hívni
                db_saved.set()
            except Exception as e:
                db_error[0] = e
                db_saved.set()
        
        db_thread = self._start_db_thread(save_db_async, name="SaveDBAsync")
        
        # Várunk az adatbázis mentés befejezésére, de közben frissítjük a GUI-t és feldolgozzuk a progress üzeneteket
        timeout_start = time.time()
        timeout_max = 600  # Maximum 10 perc timeout (hidegindításnál sok videó esetén hosszabb lehet)
        while not db_saved.is_set():
            # Timeout ellenőrzés
            if time.time() - timeout_start > timeout_max:
                self.root.after(0, lambda: self.db_notification_label.config(text="⚠ Adatbázis timeout", foreground="orange"))
                self.root.update()
                break
            
            # Feldolgozzuk az adatbázis progress üzeneteket (notification-ban jelenítjük meg)
            try:
                while True:
                    msg = self.encoding_queue.get_nowait()
                    if msg[0] == "db_progress":
                        # Notification-ban jelenítjük meg, ne a status_label-ban
                        self.root.after(0, lambda m=msg[1]: self.db_notification_label.config(text=m, foreground="blue"))
                        self.root.update()
            except queue.Empty:
                pass
            
            # Ellenőrizzük, hogy a thread még fut-e
            if not db_thread.is_alive() and not db_saved.is_set():
                # Thread leállt, de nem hívta meg a set()-et - hiba történt
                if db_error[0] is None:
                    db_error[0] = Exception("Adatbázis mentés thread váratlanul leállt")
                db_saved.set()
                break
            
            self.root.update()  # GUI frissítés
            time.sleep(0.1)  # 100ms várakozás
        
        if db_error[0]:
            self.root.after(0, lambda: self.db_notification_label.config(text=f"✗ Adatbázis hiba", foreground="red"))
        # Sikeres mentés esetén a db_progress üzenetek automatikusan megjelennek a notification label-ban
        self.root.update()  # GUI frissítés
        
        # 3. LÉPÉS: Kódolás indítása - beállítások naplózva a worker szálakban
        self.is_encoding = True
        self.encoding_worker_running = True
        self.current_video_index = -1
        
        # Status frissítése - kódolás indítása
        self.status_label.config(text="Kódolás indítása...")
        self.root.update()
        
        # VMAF/PSNR worker indítása, ha van VMAF task a queue-ban
        if not VMAF_QUEUE.empty():
            # KRITIKUS: Ha STOP_EVENT be van állítva, töröljük, mert különben a VMAF/PSNR worker azonnal kilép
            if STOP_EVENT.is_set():
                STOP_EVENT.clear()
            if not hasattr(self, 'vmaf_thread') or not self.vmaf_thread.is_alive():
                self.vmaf_thread = threading.Thread(target=self.vmaf_worker, daemon=True)
                self.vmaf_thread.start()

        # SVT-AV1 worker indítása, ha van SVT task a queue-ban
        if not SVT_QUEUE.empty():
            if not hasattr(self, 'svt_thread') or not self.svt_thread.is_alive():
                self.svt_thread = threading.Thread(target=self.svt_worker, daemon=True)
                self.svt_thread.start()

        if not AUDIO_EDIT_QUEUE.empty():
            if not self.audio_edit_thread or not self.audio_edit_thread.is_alive():
                self.audio_edit_thread = threading.Thread(target=self.audio_edit_worker, daemon=True)
                self.audio_edit_thread.start()

        # NVENC queue törlése (újratervezéskor újra soroljuk őket)
        try:
            while not NVENC_QUEUE.empty():
                NVENC_QUEUE.get_nowait()
                NVENC_QUEUE.task_done()
        except queue.Empty:
            pass
        
        # Több workeres NVENC megoldás: több nvenc_worker thread indítása
        nvenc_worker_count = self.get_configured_nvenc_workers()
        self.nvenc_worker_threads = []
        for worker_idx in range(nvenc_worker_count):
            nvenc_thread = threading.Thread(target=self.nvenc_worker, args=(worker_idx,), daemon=True)
            nvenc_thread.start()
            self.nvenc_worker_threads.append(nvenc_thread)
        
        threading.Thread(target=self.encoding_worker, daemon=True).start()
        self.root.after(100, self.check_encoding_queue)
        
    def stop_encoding_immediate(self):
        """Stop encoding immediately.
        
        Terminates all worker threads and subprocesses immediately.
        """
        # VMAF számítás ellenőrzése
        has_vmaf_work = not VMAF_QUEUE.empty() or (hasattr(self, 'vmaf_thread') and self.vmaf_thread.is_alive())
        
        if not self.is_encoding and not has_vmaf_work:
            return

        self.is_encoding = False
        self.encoding_worker_running = False
        self.graceful_stop_requested = False
        STOP_EVENT.set()
        self.status_label.config(text="Azonnali leállítás...")
        # Start gomb visszaállítása "Start"-ra
        self.start_button.config(text=t('btn_start'), command=self.start_encoding, state=tk.DISABLED)
        self.update_start_button_state()
        self.immediate_stop_button.config(state=tk.DISABLED)
        # Videók betöltése gomb aktívvá tétele
        self.load_videos_btn.config(state=tk.NORMAL)
        
        # Adatbázis mentés leállítás után
        def save_db_after_immediate_stop():
            try:
                self.save_state_to_db()
                # Notification megjelenítése
                self.root.after(0, self.show_db_notification)
            except Exception as e:
                if LOAD_DEBUG:
                    load_debug_log(f"[save_db after immediate stop] Hiba: {e}")
        self._start_db_thread(save_db_after_immediate_stop, name="SaveDBAfterImmediateStop")
        
        # Összes aktív/folyamatban lévő videó státuszának visszaállítása "NVENC queue-ban vár..."-ra vagy t('status_svt_queue')-ra
        videos_reset = []
        for video_path, item_id in self.video_items.items():
            current_values = self.tree.item(item_id, 'values')
            current_status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
            current_tag = self.tree.item(item_id, 'tags')

            # Kész vagy ellenőrizendő állapotot nem bolygatunk
            status_code = normalize_status_to_code(current_status)
            if status_code in ('completed', 'completed_nvenc', 'completed_svt', 'completed_copy', 'completed_exists', 'needs_check', 'needs_check_nvenc', 'needs_check_svt'):
                continue

            if status_code in ('audio_edit_queue', 'audio_editing'):
                original_info = self.audio_edit_task_info.get(item_id)
                self._restore_audio_task_state(item_id, original_info)
                self.audio_edit_task_info.pop(item_id, None)
                continue

            # Ha encoding állapotban van (NVENC vagy SVT-AV1), visszaállítjuk queue-ba
            if ("encoding" in current_tag or status_code in ('nvenc_encoding', 'nvenc_validation', 'nvenc_crf_search', 'svt_encoding', 'svt_validation', 'svt_crf_search', 'nvenc_queue', 'svt_queue')):
                
                orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
                completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                
                # Státusz visszaállítása: SVT queue-ban lévő esetén SVT, egyébként NVENC
                if status_code in ('svt_encoding', 'svt_validation', 'svt_crf_search', 'svt_queue'):
                    self.encoding_queue.put(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                else:
                    self.encoding_queue.put(("update", item_id, t('status_nvenc_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                self.encoding_queue.put(("tag", item_id, "pending"))
                videos_reset.append((video_path, item_id))
                
                # KRITIKUS: A fájl törlését NEM itt végezzük, hanem a processek leállítása UTÁN!
                # (A fájl törlés a stop_processes_thread() függvényben történik, hogy biztosan ne legyen lockolva)
        
        # Audio queue törlése, ha nincs futó worker
        if not (self.audio_edit_thread and self.audio_edit_thread.is_alive()):
            self._reset_audio_tasks_pending()
            self.audio_edit_thread = None
        self.audio_edit_only_mode = False

        # Adatbázis mentés a frissített állapottal
        if videos_reset:
            self.save_state_to_db()
        
        # Processek lekérése thread-safe módon (globális listából)
        with ACTIVE_PROCESSES_LOCK:
            processes_to_stop = list(ACTIVE_PROCESSES)
        
        if not processes_to_stop:
            # Nincs aktív process, csak az is_encoding flag-et állítjuk
            # Start gomb visszaállítása "Start"-ra
            self.start_button.config(text=t('btn_start'), command=self.start_encoding, state=tk.NORMAL)
            self.immediate_stop_button.config(state=tk.DISABLED)
            # Videók betöltése gomb aktívvá tétele
            self.load_videos_btn.config(state=tk.NORMAL)
            self.status_label.config(text="Leállítva")
            self.graceful_stop_requested = False
            return
        
        # Azonnali leállítás során aktív videók mappáinak gyűjtése (cleanup-hoz)
        active_video_dirs = set()
        for video_path, item_id in self.video_items.items():
            current_values = self.tree.item(item_id, 'values')
            current_status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
            current_tag = self.tree.item(item_id, 'tags')
            status_code = normalize_status_to_code(current_status)
            # Ha encoding állapotban van (kódolás vagy CRF keresés), gyűjtsük a mappáját
            if ("encoding" in current_tag or status_code in ('nvenc_encoding', 'nvenc_validation', 'nvenc_crf_search', 'svt_encoding', 'svt_validation', 'svt_crf_search')):
                video_dir = video_path.parent
                active_video_dirs.add(video_dir)
        
        # Leállítás folyamat futtatása külön thread-ben, hogy ne blokkolja a GUI-t
        def stop_processes_thread():
            # Leállítás: processek leállítása (naplózás kikommentezve)
            # videos_reset változó elérhető a closure miatt
            
            # 1. lépés: Szabályos leállítás kísérlet (terminate)
            for process in processes_to_stop:
                if process and process.poll() is None:  # Még fut
                    try:
                        process.terminate()
                    except Exception:
                        pass
            
            # Várakozás rövid ideig (3 másodperc)
            time.sleep(3)
            
            # 2. lépés: Ellenőrzés - még futnak-e?
            still_running = []
            for process in processes_to_stop:
                if process and process.poll() is None:
                    still_running.append(process)
            
            # 3. lépés: Ha még futnak, brute force kill (gyerek processekkel együtt)
            if still_running:
                for process in still_running:
                    try:
                        pid = process.pid
                        # Windows-on taskkill használata gyerek processekkel együtt
                        if platform.system() == 'Windows':
                            try:
                                # taskkill /T /F /PID <pid> - kilöli a processet és a gyerekeit is
                                subprocess.run(['taskkill', '/T', '/F', '/PID', str(pid)], 
                                             stdout=subprocess.DEVNULL, 
                                             stderr=subprocess.DEVNULL, 
                                             timeout=5)
                            except (subprocess.SubprocessError, OSError, FileNotFoundError):
                                # Ha taskkill nem sikerül, próbáljuk meg a normál kill-t
                                process.kill()
                        else:
                            # Unix/Linux/Mac - kill() használata
                            process.kill()
                            # Várakozás a befejezésre
                            try:
                                process.wait(timeout=2)
                            except (subprocess.TimeoutExpired, OSError):
                                pass
                    except Exception:
                        pass
            
            # Process listák frissítése – csak azokat vesszük ki, amelyeket kezeltünk
            with ACTIVE_PROCESSES_LOCK:
                for proc in processes_to_stop:
                    if proc in ACTIVE_PROCESSES:
                        ACTIVE_PROCESSES.remove(proc)

            # Ha maradt még futó process, ismételt kill kísérlet
            attempts = 0
            while True:
                with ACTIVE_PROCESSES_LOCK:
                    remaining = [p for p in ACTIVE_PROCESSES if p and p.poll() is None]
                if not remaining:
                    break
                for proc in remaining:
                    try:
                        pid = proc.pid
                        # Windows-on taskkill használata gyerek processekkel együtt
                        if platform.system() == 'Windows':
                            try:
                                # taskkill /T /F /PID <pid> - kilöli a processet és a gyerekeit is
                                subprocess.run(['taskkill', '/T', '/F', '/PID', str(pid)], 
                                             stdout=subprocess.DEVNULL, 
                                             stderr=subprocess.DEVNULL, 
                                             timeout=5)
                            except (subprocess.SubprocessError, OSError, FileNotFoundError):
                                # Ha taskkill nem sikerül, próbáljuk meg a normál kill-t
                                proc.kill()
                        else:
                            # Unix/Linux/Mac - kill() használata
                            proc.kill()
                            try:
                                proc.wait(timeout=2)
                            except (subprocess.TimeoutExpired, OSError):
                                pass
                    except (OSError, subprocess.SubprocessError, AttributeError):
                        pass
                with ACTIVE_PROCESSES_LOCK:
                    ACTIVE_PROCESSES[:] = [p for p in ACTIVE_PROCESSES if p and p.poll() is None]
                time.sleep(0.2)
                attempts += 1
                if attempts >= 5:
                    # Többszöri kill kísérlet után is vannak futó processek
                    break
            
            # Processek leállása után: várunk egy kicsit, hogy biztosan leálltak és felszabadultak a fájlok
            time.sleep(1)
            
            # Félkész output fájlok törlése (csak ha nem DEBUG_MODE)
            # KRITIKUS: Ezt csak a processek leállítása UTÁN végezzük, hogy biztosan ne legyen lockolva!
            if not DEBUG_MODE and videos_reset:
                for video_path, item_id in videos_reset:
                    output_file = self.video_to_output.get(video_path)
                    if output_file and output_file.exists():
                        try:
                            output_file.unlink()
                        except (OSError, PermissionError, FileNotFoundError):
                            # Ha még mindig lockolva van, próbáljuk meg később is
                            pass
            
            # .ab-av1-* mappák törlése az aktív videók mappáiból
            if active_video_dirs:
                for video_dir in active_video_dirs:
                    try:
                        cleanup_ab_av1_temp_dirs(video_dir)
                    except Exception:
                        pass  # Ha nem sikerül törölni, folytatjuk
            
            # GUI frissítés a fő thread-ben
            def on_stop_done():
                # Start gomb visszaállítása "Start"-ra
                self.start_button.config(text=t('btn_start'), command=self.start_encoding, state=tk.NORMAL)
                self.immediate_stop_button.config(state=tk.DISABLED)
                # Videók betöltése gomb aktívvá tétele
                self.load_videos_btn.config(state=tk.NORMAL)
                self.status_label.config(text="Leállítva")
                self.graceful_stop_requested = False

            self.root.after(0, on_stop_done)
        
        # Leállítás folyamat indítása külön thread-ben
        threading.Thread(target=stop_processes_thread, daemon=True).start()
    
    def stop_encoding_graceful(self):
        """Stop encoding gracefully.
        
        Signals workers to stop after finishing their current task.
        """
        # VMAF számítás ellenőrzése
        has_vmaf_work = not VMAF_QUEUE.empty() or (hasattr(self, 'vmaf_thread') and self.vmaf_thread.is_alive())
        
        if not self.is_encoding and not has_vmaf_work:
            return
        if self.graceful_stop_requested:
            return

        self.graceful_stop_requested = True
        self.status_label.config(text="Leállítás folyamatban – aktuális kódolás befejezése...")
        # Start gomb (ami most "Leállítás" gombként működik) letiltva, mert már leállítás folyamatban
        # Az update_start_button_state automatikusan kezeli ezt a graceful_stop_requested flag alapján
        self.update_start_button_state()
        # Azonnali leállítás aktív marad
        self.immediate_stop_button.config(state=tk.NORMAL)
        
    def find_next_waiting_video(self):
        """Megkeresi az első videót, ami NVENC queue-ban vár (nem SVT-AV1 queue-ban) - sorszám szerint rendezve"""
        # Ha az NVENC nincs engedélyezve, ne keressünk NVENC queue-ban várakozó videókat
        if not self.nvenc_enabled.get():
            return None
        
        # Összegyűjtjük az összes pending videót sorszám szerint rendezve
        pending_videos = []
        
        # Optimalizálás: csak a szükséges videókat ellenőrizzük (gyorsabb)
        for video_path, item_id in self.video_items.items():
            try:
                # Először gyors ellenőrzések (tree.item hívások)
                current_values = self.tree.item(item_id, 'values')
                current_status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                current_tags = self.tree.item(item_id, 'tags')
                
                # Kész vagy ellenőrizendő állapotot kihagyjuk (gyors ellenőrzés)
                if "✓ Kész" in current_status or "completed" in current_tags or "Ellenőrizendő" in current_status or "needs_check" in current_tags:
                    continue
                
                # Folyamatban lévő kódolásokat kihagyjuk (gyors ellenőrzés)
                if ("encoding" in current_tags or "encoding_nvenc" in current_tags or "encoding_svt" in current_tags or 
                    "NVENC kódolás" in current_status or "NVENC CRF keresés" in current_status or 
                    "NVENC validálás" in current_status or "SVT-AV1" in current_status):
                    continue
                
                # Csak azokat vesszük figyelembe, amelyek NVENC queue-ban várnak (nem SVT-AV1)
                status_code = normalize_status_to_code(current_status)
                if status_code != 'nvenc_queue' and not ('pending' in current_tags and status_code != 'svt_queue'):
                    continue  # Gyors skip, ha nem NVENC queue
                
                # Ellenőrizzük, hogy a videó már nincs-e feldolgozás alatt (gyors lock ellenőrzés)
                with self.nvenc_selection_lock:
                    if video_path in self.nvenc_processing_videos:
                        continue
                
                # Lassú fájlrendszer műveletek csak akkor, ha már átment a gyors ellenőrzéseken
                # Ellenőrizzük, hogy a videó létezik-e (lassú, de szükséges)
                if not video_path.exists():
                    continue
                
                # Output fájl ellenőrzés (lassú, de szükséges)
                output_file = get_output_filename(video_path, self.source_path, self.dest_path)
                if output_file.exists():
                    # Ellenőrizzük a fájl méretét (gyors ellenőrzés)
                    try:
                        file_size = output_file.stat().st_size
                        if file_size == 0:
                            # 0 byte fájl, törölni kell, de még nem kész
                            order_num = self.video_order.get(video_path, 999999)
                            pending_videos.append((order_num, video_path))
                            continue
                    except (OSError, AttributeError):
                        pass
                    # Fájl létezik, skip-eljük
                    continue
                
                # Nincs kész fájl, kódolni kell
                order_num = self.video_order.get(video_path, 999999)
                pending_videos.append((order_num, video_path))
                
                # Optimalizálás: ha már találtunk egy videót, és nincs sorszám követelmény, visszaadhatjuk azonnal
                # De mivel sorszám szerint kell rendezni, folytatjuk a keresést
            except (tk.TclError, KeyError, AttributeError, IndexError) as e:
                # Ha hiba van egy videó ellenőrzésekor, folytatjuk a következővel
                continue
        
        # Sorszám szerint rendezzük és visszaadjuk az elsőt
        if pending_videos:
            pending_videos.sort(key=lambda x: x[0])  # Sorszám szerint rendezés
            return pending_videos[0][1]  # Az első videó
        
        return None

    def encoding_worker(self):
        debug_pause.gui_queue = self.encoding_queue

        # Nem-videó fájlok másolása már a start_encoding-ban megtörtént,
        # itt csak a kódolás worker logikája következik

        completed = 0
        failed = 0
        needs_check = 0

        initial_min_vmaf = self.min_vmaf.get()
        vmaf_step = self.vmaf_step.get()
        resize_enabled = self.resize_enabled.get()
        resize_height = self.resize_height.get()
        audio_compression_enabled = self.audio_compression_enabled.get()
        audio_compression_method = self.audio_compression_method.get()
        # Ha a combobox értéke fordított szöveg, konvertáljuk
        if audio_compression_method == t('audio_compression_fast'):
            audio_compression_method = 'fast'
        elif audio_compression_method == t('audio_compression_dialogue'):
            audio_compression_method = 'dialogue'

        # SVT-AV1 worker thread indítása
        if not hasattr(self, 'svt_thread') or not self.svt_thread.is_alive():
            self.svt_thread = threading.Thread(target=self.svt_worker, daemon=True)
            self.svt_thread.start()

        stop_requested = False

        # Végigmegyünk a videókon, és mindig az első várakozót választjuk
        # Optimalizálás: késleltetjük a ciklust, hogy ne blokkolja a GUI-t
        processed_videos = 0
        last_gui_update = time.time()
        
        while True:
            if stop_requested or STOP_EVENT.is_set():
                break
            if not self.is_encoding:
                # Leállítás esetén minden várakozó videó státuszát visszaállítjuk
                # Optimalizálás: csak az első néhány videót állítjuk vissza egyszerre, hogy ne blokkolja a GUI-t
                videos_to_reset = list(self.video_files)[:100]  # Csak az első 100-at egyszerre
                for video_path in videos_to_reset:
                    if video_path in self.video_items:
                        item_id = self.video_items[video_path]
                        current_values = self.tree.item(item_id, 'values')
                        current_status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                        current_tags = self.tree.item(item_id, 'tags')
                        # Kész vagy ellenőrizendő állapotot nem bolygatunk
                        if "✓ Kész" not in current_status and "completed" not in current_tags and "Ellenőrizendő" not in current_status and "needs_check" not in current_tags:
                            orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
                            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                            # Ha SVT queue-ban volt, akkor t('status_svt_queue'), egyébként "NVENC queue-ban vár..."
                            if "SVT-AV1" in current_status and "queue" in current_status.lower():
                                self.encoding_queue.put(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                            else:
                                self.encoding_queue.put(("update", item_id, t('status_nvenc_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                            self.encoding_queue.put(("tag", item_id, "pending"))
                
                # JSON mentés a frissített állapottal
                # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                break

            # Graceful stop ellenőrzése - ne indítsunk új feladatot, ha leállítás kérvényezve van
            if self.graceful_stop_requested:
                # Várunk, amíg a folyamatban lévő feladatok befejeződnek
                time.sleep(0.5)
                # Ellenőrizzük, hogy van-e még aktív worker vagy queue-ban várakozó feladat
                has_active_nvenc_workers = len(self.nvenc_worker_threads) > 0 and any(t.is_alive() for t in self.nvenc_worker_threads)
                has_svt_queue_items = not SVT_QUEUE.empty()
                has_active_svt_worker = hasattr(self, 'svt_thread') and self.svt_thread.is_alive() if hasattr(self, 'svt_thread') else False
                nvenc_queue_size = NVENC_QUEUE.qsize()
                
                # Ha nincs több aktív worker és nincs queue-ban várakozó feladat, kilépünk
                if not has_active_nvenc_workers and not has_svt_queue_items and not has_active_svt_worker and nvenc_queue_size == 0:
                    break
                # Folytatjuk a várakozást, de NEM keressük a következő videót
                continue
            # KRITIKUS JAVÍTÁS: Graceful stop ellenőrzése ÚJRA a videó kiválasztása ELŐTT
            # Ez biztosítja, hogy NE kezdjen bele új fájlok átkódolásába
            if self.graceful_stop_requested:
                # Ha graceful stop kérvényezve van, NE keressünk új videót
                # Várunk, amíg a folyamatban lévő feladatok befejeződnek
                time.sleep(0.5)
                continue


            # Megkeressük az első várakozó videót
            # Optimalizálás: késleltetjük a keresést, hogy ne blokkolja a GUI-t
            # És hogy ne legyen másodpercenkénti státusz frissítés (sárga kijelölés elkerülése)
            current_time = time.time()
            if current_time - last_gui_update >= 0.1:  # 100ms késleltetés
                time.sleep(0.01)  # Kis késleltetés, hogy a GUI frissülhessen
                last_gui_update = current_time
            
            # Ellenőrizzük, hogy van-e szabad NVENC worker slot
            # Csak akkor keressük a következő videót, ha van szabad worker
            has_active_nvenc_workers = len(self.nvenc_worker_threads) > 0 and any(t.is_alive() for t in self.nvenc_worker_threads)
            nvenc_queue_size = NVENC_QUEUE.qsize()
            nvenc_worker_count = self.get_configured_nvenc_workers()
            
            # Csak akkor keressük a következő videót, ha van szabad worker slot
            # (queue méret < worker szám, vagy nincs aktív worker)
            if has_active_nvenc_workers and nvenc_queue_size >= nvenc_worker_count:
                # Nincs szabad worker slot, várunk
                time.sleep(0.5)
                continue
            
            video_path = self.find_next_waiting_video()
            if video_path is None:
                # Nincs több várakozó NVENC videó
                # Ellenőrizzük, hogy van-e még SVT queue-ban várakozó videó vagy aktív NVENC/SVT worker
                has_svt_queue_items = not SVT_QUEUE.empty()
                has_active_svt_worker = hasattr(self, 'svt_thread') and self.svt_thread.is_alive() if hasattr(self, 'svt_thread') else False
                
                if has_svt_queue_items or has_active_nvenc_workers or has_active_svt_worker:
                    # Van még feldolgozás alatt lévő videó, várunk
                    time.sleep(0.5)
                    # Újra próbáljuk, hátha új videó került a queue-ba
                    video_path = self.find_next_waiting_video()
                    if video_path is None:
                        # Még mindig nincs NVENC videó, de lehet, hogy SVT worker még dolgozik
                        # Várunk, amíg a queue-k kiürülnek
                        continue
                else:
                    # Nincs több várakozó videó és nincs aktív worker
                    break

            # Graceful stop ellenőrzése - ne feldolgozzuk a videót, ha leállítás kérvényezve van
            if self.graceful_stop_requested:
                # Várunk, amíg a folyamatban lévő feladatok befejeződnek
                time.sleep(0.5)
                # Ellenőrizzük, hogy van-e még aktív worker vagy queue-ban várakozó feladat
                has_active_nvenc_workers = len(self.nvenc_worker_threads) > 0 and any(t.is_alive() for t in self.nvenc_worker_threads)
                has_svt_queue_items = not SVT_QUEUE.empty()
                has_active_svt_worker = hasattr(self, 'svt_thread') and self.svt_thread.is_alive() if hasattr(self, 'svt_thread') else False
                nvenc_queue_size = NVENC_QUEUE.qsize()
                
                # Ha nincs több aktív worker és nincs queue-ban várakozó feladat, kilépünk
                if not has_active_nvenc_workers and not has_svt_queue_items and not has_active_svt_worker and nvenc_queue_size == 0:
                    break
                # Folytatjuk a várakozást, de NEM feldolgozzuk a videót
                continue

            # A "VIDEÓ X/Y" üzenet a worker-ben lesz kiírva, ahol már tudjuk, hogy melyik worker dolgozik rajta
            # Így elkerüljük, hogy minden üzenet az első logger-be menjen
            
            # Ellenőrizzük, hogy a forrás videó létezik-e
            if not video_path.exists():
                with console_redirect(self.nvenc_logger):
                    print(f"⚠ Hiba: A forrás videó nem található: {video_path}")
                if video_path in self.video_items:
                    item_id = self.video_items[video_path]
                    current_values = self.tree.item(item_id, 'values')
                    completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                    self.encoding_queue.put(("update", item_id, "✗ Forrás videó hiányzik", "-", "-", "-", "-", "-", "-", "-", completed_date))
                    self.encoding_queue.put(("tag", item_id, "failed"))
                    # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                failed += 1
                self.encoding_queue.put(("progress_bar", completed + failed + needs_check))
                continue

            output_file = get_output_filename(video_path, self.source_path, self.dest_path)
            if output_file.exists():
                # Ellenőrizzük a fájl méretét és időtartamát
                file_size = output_file.stat().st_size
                should_delete = False
                
                # Ha 0 byte méretű, törölhetjük
                if file_size == 0:
                    should_delete = True
                    with console_redirect(self.nvenc_logger):
                        print(f"⚠ Célfájl 0 byte méretű, törlés és újrakódolás: {output_file.name}")
                else:
                    # Ellenőrizzük az időtartamot
                    source_duration, _ = get_video_info(video_path)
                    output_duration, _ = get_video_info(output_file)
                    
                    if source_duration is not None and output_duration is not None:
                        # Ha a célfájl rövidebb, mint a forrás (több mint 1 másodperc különbség), töröljük
                        if output_duration < source_duration - 1.0:
                            should_delete = True
                            with console_redirect(self.nvenc_logger):
                                output_duration_str = format_localized_number(output_duration, decimals=1) if output_duration is not None else "-"
                                source_duration_str = format_localized_number(source_duration, decimals=1) if source_duration is not None else "-"
                                print(f"⚠ Célfájl rövidebb ({output_duration_str}s) mint a forrás ({source_duration_str}s), törlés és újrakódolás: {output_file.name}")
                
                if should_delete:
                    try:
                        output_file.unlink()
                    except Exception as e:
                        with console_redirect(self.nvenc_logger):
                            print(f"✗ Hiba a fájl törlésekor: {e}")
                else:
                    # Fájl rendben van, skip-eljük
                    completed += 1
                    self.encoding_queue.put(("progress_bar", completed + failed + needs_check))
                    continue

            # Az aktuális videó indexét beállítjuk (opcionális, csak debug célra)
            if video_path in self.video_files:
                self.current_video_index = self.video_files.index(video_path)
            item_id = self.video_items[video_path]
            current_values = self.tree.item(item_id)['values']
            orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
            current_status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""

            # Ha a betöltött státusz már SVT-AV1 queue-ban vár, akkor automatikusan SVT queue-ba helyezzük
            # Ne próbáljuk újra NVENC-cel
            if "SVT-AV1" in current_status and ("queue-ban vár" in current_status or "várakozás" in current_status.lower() or "vár" in current_status.lower()):
                with console_redirect(self.svt_logger):
                    print(f"\n⚠ Videó már SVT-AV1 queue-ban van (betöltött státusz) → SVT queue-ba újrahelyezés: {video_path.name}")
                
                valid_subtitles, invalid_subtitles = self._get_validated_subtitles_for_video(video_path)
                subtitle_files = valid_subtitles
                max_encoded = self.max_encoded_percent.get()
                
                svt_task = {
                    'video_path': video_path,
                    'output_file': output_file,
                    'subtitle_files': subtitle_files,
                    'invalid_subtitles': invalid_subtitles,
                    'item_id': item_id,
                    'orig_size_str': orig_size_str,
                    'initial_min_vmaf': initial_min_vmaf,
                    'vmaf_step': vmaf_step,
                    'max_encoded': max_encoded,
                    'resize_enabled': self.resize_enabled.get(),
                    'resize_height': self.resize_height.get(),
                    'audio_compression_enabled': self.audio_compression_enabled.get(),
                    'audio_compression_method': self.audio_compression_method.get(),
                    'reason': 'resume_from_json'
                }
                # Graceful stop ellenőrzése - ne indítsunk új feladatot, ha leállítás kérvényezve van
                if self.graceful_stop_requested:
                    # Ne indítsunk új feladatot, ha graceful stop kérvényezve van
                    continue
                
                SVT_QUEUE.put(svt_task)
                completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                self.encoding_queue.put(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                self.encoding_queue.put(("tag", item_id, "encoding_svt"))
                # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik  # JSON mentés SVT queue-ba kerülés után
                continue

            valid_subtitles, invalid_subtitles = self._get_validated_subtitles_for_video(video_path)
            subtitle_files = valid_subtitles
            max_encoded = self.max_encoded_percent.get()

            def status_callback(msg):
                self.encoding_queue.put(("status_only", item_id, msg))

            def progress_callback(msg):
                self.encoding_queue.put(("progress", item_id, msg))
                # Becsült befejezési idő számítása a progress alapján (frame szám alapján számolódik)
                self.update_estimated_end_time_from_progress(item_id, msg)

            # Normál folyamat - nincs task itt, ez a normál video_files feldolgozás
            # skip_crf_search csak a manuális újrakódolásnál van, ami külön worker-ben történik
            
            if self.graceful_stop_requested:
                break

            # Kezdeti státusz a cél VMAF értékkel
            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
            localized_vmaf = format_localized_number(initial_min_vmaf, decimals=1)
            self.encoding_queue.put(("update", item_id, f"NVENC CRF keresés (VMAF: {localized_vmaf})...", "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
            
            # Kezdési időpont tárolása
            self.encoding_start_times[item_id] = time.time()

            # KRITIKUS: Előbb hozzáadjuk a videót a processing set-hez, hogy ne lehessen újra kiválasztani
            with self.nvenc_selection_lock:
                if video_path in self.nvenc_processing_videos:
                    # Már feldolgozás alatt van, kihagyjuk
                    continue
                self.nvenc_processing_videos.add(video_path)
            
            # Státusz frissítése: NVENC queue-ban vár (pending tag - kék szín)
            current_values = self.tree.item(item_id, 'values')
            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
            # Graceful stop ellenőrzése - ne indítsunk új feladatot, ha leállítás kérvényezve van
            if self.graceful_stop_requested:
                # Ne indítsunk új feladatot, ha graceful stop kérvényezve van
                # Eltávolítjuk a videót a processing set-ből, hogy ne maradjon ott
                with self.nvenc_selection_lock:
                    self.nvenc_processing_videos.discard(video_path)
                continue
            
            self.encoding_queue.put(("update", item_id, t('status_nvenc_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
            self.encoding_queue.put(("tag", item_id, "pending"))
            # Várunk egy kicsit, hogy a GUI frissüljön
            time.sleep(0.01)
            
            # Több workeres NVENC megoldás: NVENC queue-ba tesszük a feladatot
            nvenc_task = {
                'video_path': video_path,
                'output_file': output_file,
                'subtitle_files': subtitle_files,
                'invalid_subtitles': invalid_subtitles,
                'item_id': item_id,
                'orig_size_str': orig_size_str,
                'initial_min_vmaf': initial_min_vmaf,
                'vmaf_step': vmaf_step,
                'max_encoded': max_encoded,
                'resize_enabled': resize_enabled,
                'resize_height': resize_height,
                'audio_compression_enabled': audio_compression_enabled,
                'audio_compression_method': audio_compression_method,
                'reason': 'start_encoding'
            }
            NVENC_QUEUE.put(nvenc_task)
            # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
            continue

        # Várakozás: NVENC és SVT-AV1 queue-ban lévő összes feladat feldolgozása
        NVENC_QUEUE.join()
        SVT_QUEUE.join()
        
        if stop_requested or STOP_EVENT.is_set():
            with console_redirect(self.nvenc_logger):
                print(f"\n🛑 Azonnali leállítás – encoding worker megszakítva\n")
            return

        if self.graceful_stop_requested:
            with console_redirect(self.nvenc_logger):
                print(f"\n🟡 Leállítás kérése – új feladatok nem indulnak\n")
            # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
            self.encoding_queue.put(("paused", completed, failed, needs_check))
            return

        with console_redirect(self.nvenc_logger):
            print(f"\n{'#'*80}\n### ENCODING WORKER KÉSZ ###\n{'#'*80}\n")
            print(f"⏳ Várakozás SVT-AV1 queue feldolgozására...\n")
            print(f"\n{'#'*80}\n### ÖSSZES FELDOLGOZÁS KÉSZ ###\n{'#'*80}\n")
            print(f"Végeredmény:")
            print(f"  - NVENC kódolások: {completed} OK, {needs_check} ellenőrizendő, {failed} hiba")
        
        # Végső statisztika
        self.encoding_queue.put(("finished", completed, failed, needs_check))
        
    def check_encoding_queue(self):
        """Process messages from the encoding queue.
        
        Main GUI update loop. Handles log messages, status updates, and debug events
        from worker threads.
        """
        try:
            while True:
                msg = self.encoding_queue.get_nowait()

                if msg[0] == "nvenc_log":
                    # Üzenet formátum: ("nvenc_log", worker_idx, logger_idx, log_msg) vagy régi formátum: ("nvenc_log", worker_idx, log_msg)
                    if len(msg) == 4:
                        _, worker_idx, logger_idx, log_msg = msg
                    elif len(msg) == 3:
                        _, worker_idx, log_msg = msg
                        logger_idx = worker_idx  # Régi formátum: logger_idx = worker_idx
                    else:
                        _, log_msg = msg
                        worker_idx = 0
                        logger_idx = 0
                    target_console = None
                    if hasattr(self, 'nvenc_consoles') and self.nvenc_consoles:
                        if logger_idx is None or logger_idx < 0:
                            logger_idx = 0
                        # Logger index alapján választunk (nem worker_index!), hogy elkerüljük a race condition-t
                        if len(self.nvenc_consoles) > 0:
                            console_idx = logger_idx % len(self.nvenc_consoles)
                            target_console = self.nvenc_consoles[console_idx]
                    if target_console is None and hasattr(self, 'nvenc_console'):
                        target_console = self.nvenc_console
                    if target_console is not None:
                        target_console.config(state=tk.NORMAL)
                        target_console.insert(tk.END, log_msg)
                        target_console.see(tk.END)
                        target_console.config(state=tk.DISABLED)
                    continue
                elif msg[0] == "svt_log":
                    _, log_msg = msg
                    self.svt_console.config(state=tk.NORMAL)
                    self.svt_console.insert(tk.END, log_msg)
                    self.svt_console.see(tk.END)
                    self.svt_console.config(state=tk.DISABLED)
                    continue

                if msg[0] == "debug_pause":
                    if len(msg) != 5:
                        print(f"HIBA: debug_pause üzenet nem 5 paramétert tartalmaz: {len(msg)} paraméter")
                        continue
                    _, current_step, next_step, file_info, continue_event = msg
                    self.show_debug_dialog(current_step, next_step, file_info, continue_event)
                    continue

                if msg[0] == "revert_status_if_not_done":
                    if len(msg) != 4:
                        print(f"HIBA: revert_status_if_not_done üzenet nem 4 paramétert tartalmaz: {len(msg)} paraméter")
                        continue
                    _, item_id, target_status, orig_size_str = msg
                    try:
                        current_values = self.tree.item(item_id, 'values')
                        if not current_values:
                            continue
                        
                        status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                        tags = self.tree.item(item_id, 'tags')
                        
                        # Kész vagy ellenőrizendő állapotot nem bolygatunk
                        # Használjuk a helper függvényeket a státusz ellenőrzéséhez
                        if not is_status_completed(status) and "needs_check" not in tags and "Ellenőrizendő" not in status:
                            # Megőrizzük a többi értéket
                            video_name = current_values[self.COLUMN_INDEX['video_name']] if len(current_values) > self.COLUMN_INDEX['video_name'] else ""
                            duration = current_values[self.COLUMN_INDEX['duration']] if len(current_values) > self.COLUMN_INDEX['duration'] else "-"
                            frames = current_values[self.COLUMN_INDEX['frames']] if len(current_values) > self.COLUMN_INDEX['frames'] else "-"
                            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                            
                            # Frissítjük a sort
                            self.tree.item(item_id, values=(video_name, target_status, "-", "-", "-", "-", orig_size_str, "-", "-", duration, frames, completed_date), tags=("pending",))
                    except Exception as e:
                        print(f"Hiba státusz visszaállításakor: {e}")
                    continue

                if msg[0] == "update":
                    if len(msg) != 11:
                        # Ha nem 11 paraméter, akkor hiba - logoljuk és ugorjunk át
                        print(f"HIBA: update üzenet nem 11 paramétert tartalmaz: {len(msg)} paraméter")
                        continue
                    if len(msg) == 11:
                        _, item_id, status, cq, vmaf, psnr, progress, orig_size, new_size, change, completed_date = msg
                        try:
                            # Mentjük a jelenlegi kijelölést, hogy ne sárgásítsa a sort automatikusan
                            current_selection = self.tree.selection()
                            
                            # Megkeressük a video_name-t
                            current_values = self.tree.item(item_id, 'values')
                            video_name = current_values[self.COLUMN_INDEX['video_name']] if len(current_values) > self.COLUMN_INDEX['video_name'] else ""
                            # Ha nincs video_name, megpróbáljuk a video_path-ból
                            if not video_name:
                                for video_path, vid_item_id in self.video_items.items():
                                    if vid_item_id == item_id:
                                        try:
                                            video_name = self.format_relative_name(video_path)
                                        except Exception as e:
                                            log_error = f"✗ Relatív útvonal hiba (queue update): {video_path} -> {e}"
                                            if LOG_WRITER:
                                                try:
                                                    LOG_WRITER.write(log_error + "\n")
                                                    LOG_WRITER.flush()
                                                except Exception:
                                                    pass
                                            video_name = video_path.name
                                        break
                            # Megtartjuk a duration és frames értékeket
                            duration_str = current_values[self.COLUMN_INDEX['duration']] if len(current_values) > self.COLUMN_INDEX['duration'] else "-"
                            frames_str = current_values[self.COLUMN_INDEX['frames']] if len(current_values) > self.COLUMN_INDEX['frames'] else "-"
                            
                            # Frissítjük az értékeket
                            self.tree.item(item_id, values=(video_name, status, cq, vmaf, psnr, progress, orig_size, new_size, change, duration_str, frames_str, completed_date))
                            
                            # Frissítsük a tree_item_data-t az új értékekkel (gyors DB mentéshez, parse-olás nélkül)
                            if item_id not in self.tree_item_data:
                                self.tree_item_data[item_id] = {}
                            
                            # CQ érték tárolása vagy törlése
                            if cq != "-":
                                try:
                                    # Parse-oljuk a CQ-t (lehet szám vagy string)
                                    cq_val = float(cq.replace(",", ".")) if isinstance(cq, str) else float(cq)
                                    self.tree_item_data[item_id]['cq'] = cq_val
                                except (ValueError, TypeError):
                                    pass
                            else:
                                # Ha "-", töröljük a tree_item_data-ból
                                if item_id in self.tree_item_data and 'cq' in self.tree_item_data[item_id]:
                                    del self.tree_item_data[item_id]['cq']
                            
                            # VMAF érték tárolása vagy törlése
                            if vmaf != "-":
                                try:
                                    vmaf_val = float(vmaf.replace(",", ".")) if isinstance(vmaf, str) else float(vmaf)
                                    self.tree_item_data[item_id]['vmaf'] = vmaf_val
                                except (ValueError, TypeError):
                                    pass
                            else:
                                # Ha "-", töröljük a tree_item_data-ból
                                if item_id in self.tree_item_data and 'vmaf' in self.tree_item_data[item_id]:
                                    del self.tree_item_data[item_id]['vmaf']
                            
                            # PSNR érték tárolása vagy törlése
                            if psnr != "-":
                                try:
                                    psnr_val = float(psnr.replace(",", ".")) if isinstance(psnr, str) else float(psnr)
                                    self.tree_item_data[item_id]['psnr'] = psnr_val
                                except (ValueError, TypeError):
                                    pass
                            else:
                                # Ha "-", töröljük a tree_item_data-ból
                                if item_id in self.tree_item_data and 'psnr' in self.tree_item_data[item_id]:
                                    del self.tree_item_data[item_id]['psnr']
                            
                            # New size bytes tárolása vagy törlése
                            if new_size != "-" and "MB" in new_size:
                                try:
                                    # Parse-oljuk a new_size-t byte-okra
                                    new_size_bytes = parse_size_to_bytes(new_size)
                                    if new_size_bytes:
                                        self.tree_item_data[item_id]['new_size_bytes'] = new_size_bytes
                                except (ValueError, TypeError):
                                    pass
                            else:
                                # Ha "-" vagy nincs "MB", töröljük a tree_item_data-ból
                                if item_id in self.tree_item_data and 'new_size_bytes' in self.tree_item_data[item_id]:
                                    del self.tree_item_data[item_id]['new_size_bytes']
                            
                            # Ha completed státusz, frissítsük a tree_item_data-t az új output_encoder_type-pal
                            # (később újra számoljuk a status_code-ot, de itt előre kell)
                            temp_status_code = normalize_status_to_code(status)
                            if temp_status_code in ('completed', 'completed_nvenc', 'completed_svt', 'completed_copy', 'completed_exists'):
                                # Megkeressük a video_path-ot és output_file-t
                                video_path = None
                                for vp, vid in self.video_items.items():
                                    if vid == item_id:
                                        video_path = vp
                                        break
                                
                                if video_path:
                                    output_file = self.video_to_output.get(video_path)
                                    if not output_file:
                                        output_file = get_output_filename(video_path, self.source_path, self.dest_path)
                                    
                                    # Ha van output fájl, proboljuk és frissítsük a tree_item_data-t
                                    if output_file and output_file.exists():
                                        try:
                                            # Gyors probe csak az encoder_type-ért
                                            probe_cmd = [
                                                FFPROBE_PATH, '-v', 'error',
                                                '-show_entries', 'format_tags=Settings',
                                                '-of', 'default=noprint_wrappers=1:nokey=1',
                                                os.fspath(output_file.absolute())
                                            ]
                                            result_probe = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=5, startupinfo=get_startup_info())
                                            settings_str = result_probe.stdout.strip() if result_probe.stdout else ""
                                            if settings_str:
                                                output_encoder_type = None
                                                if 'NVENC' in settings_str.upper() or 'CQ:' in settings_str:
                                                    output_encoder_type = 'nvenc'
                                                elif 'SVT-AV1' in settings_str.upper() or 'SVT' in settings_str.upper() or 'CRF:' in settings_str:
                                                    output_encoder_type = 'svt-av1'
                                                
                                                # Frissítsük a tree_item_data-t
                                                if output_encoder_type:
                                                    if item_id not in self.tree_item_data:
                                                        self.tree_item_data[item_id] = {}
                                                    self.tree_item_data[item_id]['output_encoder_type'] = output_encoder_type
                                        except Exception:
                                            # Probolás hiba - nem kritikus, csak logoljuk
                                            pass
                            
                            # Visszaállítjuk a kijelölést (vagy töröljük, ha üres volt)
                            if current_selection:
                                try:
                                    self.tree.selection_set(current_selection)
                                except (tk.TclError, KeyError, AttributeError):
                                    pass
                            else:
                                # Ha nem volt kijelölés, töröljük az automatikus kijelölést
                                try:
                                    auto_selection = self.tree.selection()
                                    if auto_selection:
                                        self.tree.selection_remove(auto_selection)
                                except (tk.TclError, KeyError, AttributeError):
                                    pass
                        except (tk.TclError, KeyError, AttributeError):
                            # Item már nem létezik (pl. reload történt) - skip
                            continue
                    
                        status_code = normalize_status_to_code(status)
                        if status_code in (
                            'nvenc_queue',
                            'svt_queue',
                            'vmaf_waiting',
                            'psnr_waiting',
                            'vmaf_psnr_waiting',
                            'audio_edit_queue'
                        ):
                            self.clear_encoding_times(item_id)
                        if self.hide_completed.get():
                            if is_status_completed(status):
                                if item_id not in self.hidden_items:
                                    try:
                                        parent = self.tree.parent(item_id)
                                        if parent == "" and item_id in self.tree.get_children():
                                            self.tree.detach(item_id)
                                            self.hidden_items.add(item_id)
                                    except (tk.TclError, KeyError, AttributeError):
                                        pass
                            else:
                                self._show_hidden_item_if_needed(item_id)
                elif msg[0] == "tag":
                    _, item_id, tag = msg
                    try:
                        self.tree.item(item_id, tags=(tag,))
                    except (tk.TclError, KeyError, AttributeError):
                        # Item már nem létezik - skip
                        pass
                elif msg[0] == "progress":
                    _, item_id, progress_msg = msg
                    try:
                        # Mentjük a jelenlegi kijelölést, hogy ne sárgásítsa a sort automatikusan
                        current_selection = self.tree.selection()
                        
                        current_values = self.get_tree_values(item_id)
                        current_values[self.COLUMN_INDEX['progress']] = progress_msg
                        self.tree.item(item_id, values=tuple(current_values))
                        
                        # Visszaállítjuk a kijelölést (vagy töröljük, ha üres volt)
                        if current_selection:
                            try:
                                self.tree.selection_set(current_selection)
                            except (tk.TclError, KeyError, AttributeError):
                                pass
                        else:
                            try:
                                auto_selection = self.tree.selection()
                                if auto_selection:
                                    self.tree.selection_remove(auto_selection)
                            except (tk.TclError, KeyError, AttributeError):
                                pass
                    except (tk.TclError, KeyError, AttributeError, IndexError):
                        # Item már nem létezik - skip
                        pass
                elif msg[0] == "status_only":
                    _, item_id, status_text = msg
                    try:
                        # Mentjük a jelenlegi kijelölést, hogy ne sárgásítsa a sort automatikusan
                        current_selection = self.tree.selection()
                        
                        current_values = self.get_tree_values(item_id)
                        current_values[self.COLUMN_INDEX['status']] = status_text
                        self.tree.item(item_id, values=tuple(current_values))
                        
                        # Visszaállítjuk a kijelölést (vagy töröljük, ha üres volt)
                        if current_selection:
                            try:
                                self.tree.selection_set(current_selection)
                            except (tk.TclError, KeyError, AttributeError):
                                pass
                        else:
                            try:
                                auto_selection = self.tree.selection()
                                if auto_selection:
                                    self.tree.selection_remove(auto_selection)
                            except (tk.TclError, KeyError, AttributeError):
                                pass
                    except (tk.TclError, KeyError, AttributeError):
                        pass
                elif msg[0] == "save_json":
                    # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                    pass
                elif msg[0] == "progress_bar":
                    _, value = msg
                    # Dinamikusan számoljuk ki a befejezett videók számát a tree-ből
                    completed_count = 0
                    for item_id in self.video_items.values():
                        try:
                            tags = self.tree.item(item_id, 'tags')
                            if 'completed' in tags or 'failed' in tags or 'needs_check' in tags:
                                completed_count += 1
                        except (tk.TclError, KeyError, AttributeError):
                            # Item már nem létezik - skip
                            pass
                    
                    self.progress_bar['value'] = completed_count
                    total = len(self.video_files) if self.video_files else 1
                    percent = int((completed_count / total) * 100) if total > 0 else 0
                    remaining = total - completed_count
                    self.status_label.config(text=f"Kódolás: {completed_count}/{total} kész ({percent}%) • {remaining} hátra")
                elif msg[0] == "copy_progress":
                    # Tuple formátum: (típus, total, current, message)
                    _, copy_data = msg
                    if isinstance(copy_data, tuple) and len(copy_data) >= 4:
                        copy_type, total, current, message = copy_data[0], copy_data[1], copy_data[2], copy_data[3]
                        
                        if copy_type == "copy_start":
                            # Másolás kezdése
                            self.progress_bar['maximum'] = total
                            self.progress_bar['value'] = 0
                            self.status_label.config(text=message)
                        elif copy_type == "copy_progress":
                            # Másolás folyamatban
                            self.progress_bar['maximum'] = total
                            self.progress_bar['value'] = current
                            percent = int((current / total) * 100) if total > 0 else 0
                            self.status_label.config(text=f"{message} ({percent}%)")
                        elif copy_type == "copy_done":
                            # Másolás befejezve
                            self.progress_bar['maximum'] = total if total > 0 else 1
                            self.progress_bar['value'] = current if current > 0 else total
                            self.status_label.config(text=message)
                        elif copy_type == "copy_error":
                            # Hiba
                            self.status_label.config(text=message)
                elif msg[0] == "copy_status":
                    _, msg_text = msg
                    self.status_label.config(text=msg_text)
                elif msg[0] == "db_progress":
                    _, msg_text = msg
                    # db_progress üzeneteket a notification label-ban jelenítjük meg, ne a status_label-ban
                    if hasattr(self, 'db_notification_label'):
                        self.root.after(0, lambda m=msg_text: self.db_notification_label.config(text=m, foreground="blue"))
                elif msg[0] == "update_summary":
                    self.update_summary_row()
                elif msg[0] == "finished":
                    _, completed, failed, needs_check = msg
                    self.encoding_worker_running = False
                    has_vmaf_work = (not VMAF_QUEUE.empty()) or (hasattr(self, 'vmaf_thread') and self.vmaf_thread.is_alive())
                    parts = [f"OK: {completed}"]
                    if needs_check > 0:
                        parts.append(f"Ellenőrizendő: {needs_check}")
                    if failed > 0:
                        parts.append(f"Hiba: {failed}")
                    summary_text = ", ".join(parts)
                    if has_vmaf_work:
                        self.status_label.config(text=f"{t('status_vmaf_calculating')} - {summary_text}")
                    else:
                        self.status_label.config(text=f"{t('status_completed')} {summary_text}")
                        # Start gomb visszaállítása "Start"-ra
                        self.start_button.config(text=t('btn_start'), command=self.start_encoding, state=tk.NORMAL)
                        self.immediate_stop_button.config(state=tk.DISABLED)
                        # Videók betöltése gomb aktívvá tétele
                        self.load_videos_btn.config(state=tk.NORMAL)
                        self.is_encoding = False
                    self.graceful_stop_requested = False
                    msg_parts = [f"Kódolás befejezve!\n\nSikeres: {completed}"]
                    if needs_check > 0:
                        msg_parts.append(f"Ellenőrizendő: {needs_check}")
                    if failed > 0:
                        msg_parts.append(f"Sikertelen: {failed}")
                    if has_vmaf_work:
                        msg_parts.append(t('status_vmaf_calculating'))
                    messagebox.showinfo("Kész", '\n'.join(msg_parts))
                    # Adatbázis mentés leállítás után
                    def save_db_after_stop():
                        try:
                            self.save_state_to_db()
                            # Notification megjelenítése
                            self.root.after(0, self.show_db_notification)
                        except Exception as e:
                            if LOAD_DEBUG:
                                load_debug_log(f"[save_db after stop] Hiba: {e}")
                    self._start_db_thread(save_db_after_stop, name="SaveDBAfterStop")
                elif msg[0] == "paused":
                    _, completed, failed, needs_check = msg
                    self.is_encoding = False
                    self.encoding_worker_running = False
                    self.graceful_stop_requested = False
                    # Start gomb visszaállítása "Start"-ra
                    self.start_button.config(text=t('btn_start'), command=self.start_encoding, state=tk.NORMAL)
                    self.immediate_stop_button.config(state=tk.DISABLED)
                    # Videók betöltése gomb aktívvá tétele
                    self.load_videos_btn.config(state=tk.NORMAL)
                    # Csak akkor jelenítjük meg a részleteket, ha van valami értékes információ
                    parts = []
                    if completed > 0:
                        parts.append(f"OK: {completed}")
                    if needs_check > 0:
                        parts.append(f"Ellenőrizendő: {needs_check}")
                    if failed > 0:
                        parts.append(f"Hiba: {failed}")
                    # Ha van részlet, hozzáadjuk, különben csak a leállítás üzenet
                    if parts:
                        self.status_label.config(text=f"Leállítva (folytatáshoz 'Kódolás indítása') – {', '.join(parts)}")
                    else:
                        self.status_label.config(text="Leállítva (folytatáshoz 'Kódolás indítása')")
        except queue.Empty:
            pass

        self.update_start_button_state()

        # Mindig folytatjuk a queue ellenőrzést, még akkor is, ha nincs aktív kódolás
        # (pl. nem-videó fájlok másolása közben is frissüljön a GUI)
        copy_thread_alive = False
        if hasattr(self, 'copy_thread') and self.copy_thread:
            try:
                copy_thread_alive = self.copy_thread.is_alive()
            except (AttributeError, RuntimeError):
                copy_thread_alive = False
        
        if self.is_encoding or copy_thread_alive:
            self.root.after(100, self.check_encoding_queue)
        elif not self.encoding_queue.empty():
            # Ha van üzenet a queue-ban, akkor is folytatjuk
            self.root.after(100, self.check_encoding_queue)
    
    def process_manual_nvenc_tasks_worker(self):
        """Feldolgozza a manuális NVENC újrakódolás taskokat"""
        self.manual_nvenc_active = True
        try:
            while self.manual_nvenc_tasks:
                if self.graceful_stop_requested and not STOP_EVENT.is_set():
                    break
                if STOP_EVENT.is_set():
                    break

                task = self.manual_nvenc_tasks.pop(0)
                video_path = task['video_path']
                output_file = task['output_file']
                subtitle_files = task['subtitle_files']
                invalid_subtitles = task.get('invalid_subtitles', [])
                item_id = task['item_id']
                orig_size_str = task['orig_size_str']
                target_cq = task['target_cq']
                vmaf_value = task.get('vmaf_value', None)
                resize_enabled = task.get('resize_enabled', False)
                resize_height = task.get('resize_height', 1080)
                audio_compression_enabled = task.get('audio_compression_enabled', self.audio_compression_enabled.get())
                audio_compression_method = task.get('audio_compression_method', self.audio_compression_method.get())
                if audio_compression_method == t('audio_compression_fast'):
                    audio_compression_method = 'fast'
                elif audio_compression_method == t('audio_compression_dialogue'):
                    audio_compression_method = 'dialogue'

                # Manuális NVENC újrakódolás - közvetlenül feldolgozzuk (nincs queue)
                with console_redirect(self.nvenc_logger):
                    print(f"\n{'*'*80}\nMANUÁLIS NVENC ÚJRAKÓDOLÁS: {video_path.name}\nCQ: {target_cq}\n{'*'*80}")

                current_values = self.tree.item(item_id, 'values')
                completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                vmaf_display = format_localized_number(vmaf_value, decimals=1) if vmaf_value is not None else "-"
                self.encoding_queue.put(("update", item_id, f"NVENC kódolás... (CQ {int(target_cq)})", str(int(target_cq)), vmaf_display, "-", "-", orig_size_str, "-", "-", completed_date))
                self.encoding_queue.put(("tag", item_id, "encoding"))
                self.encoding_start_times[item_id] = time.time()

                def progress_callback(msg):
                    self.encoding_queue.put(("progress", item_id, msg))
                    self.update_estimated_end_time_from_progress(item_id, msg)

                stop_encoding = False
                success_nvenc = False
                with console_redirect(self.nvenc_logger):
                    try:
                        success_nvenc = encode_single_attempt(
                            video_path,
                            output_file,
                            target_cq,
                            subtitle_files,
                            'av1_nvenc',
                            progress_callback,
                            stop_event=STOP_EVENT,
                            vmaf_value=vmaf_value,
                            resize_enabled=resize_enabled,
                            resize_height=resize_height,
                            audio_compression_enabled=audio_compression_enabled,
                            audio_compression_method=audio_compression_method
                        )
                    except EncodingStopped:
                        stop_encoding = True

                if stop_encoding:
                    break

                if not self.is_encoding:
                    current_values = self.tree.item(item_id, 'values')
                    status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                    tags = self.tree.item(item_id, 'tags')
                    if "✓ Kész" not in status and "completed" not in tags and "Ellenőrizendő" not in status and "needs_check" not in tags:
                        orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
                        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                        self.encoding_queue.put(("update", item_id, t('status_nvenc_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                        self.encoding_queue.put(("tag", item_id, "pending"))
                        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                    break

                is_valid = False
                used_encoder = "NVENC"
                final_cq = target_cq
                final_vmaf = "-"

                if success_nvenc:
                    # KRITIKUS: Ellenőrizzük, hogy a videó már "Kész" állapotban van-e (pl. VMAF/PSNR számítás után)
                    # Ha igen, ne indítsuk újra a validálást!
                    current_values = self.tree.item(item_id, 'values')
                    status_before_validation = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                    tags_before_validation = self.tree.item(item_id, 'tags')
                    is_already_completed = (
                        "✓ Kész" in status_before_validation or 
                        "completed" in tags_before_validation or 
                        "Kész" in status_before_validation
                    )
                    
                    if is_already_completed:
                        # A videó már kész (pl. VMAF/PSNR számítás után), ne indítsuk újra a validálást!
                        continue
                    
                    completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                    self.encoding_queue.put(("update", item_id, "NVENC validálás...", str(int(target_cq)), "-", "-", "100%", orig_size_str, "-", "-", completed_date))

                    if not self.is_encoding:
                        current_values = self.tree.item(item_id, 'values')
                        status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                        tags = self.tree.item(item_id, 'tags')
                        if "✓ Kész" not in status and "completed" not in tags and "Ellenőrizendő" not in status and "needs_check" not in tags:
                            orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
                            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                            self.encoding_queue.put(("update", item_id, t('status_nvenc_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                            self.encoding_queue.put(("tag", item_id, "pending"))
                            # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                        break

                    stop_validation = False
                    try:
                        with console_redirect(self.nvenc_logger):
                            is_valid = validate_encoded_video_vlc(output_file, encoder='av1_nvenc', stop_event=STOP_EVENT, source_path=video_path)
                    except EncodingStopped:
                        stop_validation = True

                    if stop_validation:
                        break

                    # KRITIKUS: Újraellenőrizzük a validálás után is, hogy a videó már "Kész" állapotban van-e
                    # (lehet, hogy közben VMAF/PSNR számítás befejeződött)
                    current_values = self.tree.item(item_id, 'values')
                    status_after_validation = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                    tags_after_validation = self.tree.item(item_id, 'tags')
                    is_now_completed = (
                        "✓ Kész" in status_after_validation or 
                        "completed" in tags_after_validation or 
                        "Kész" in status_after_validation
                    )
                    
                    if is_now_completed:
                        # A videó közben kész lett (pl. VMAF/PSNR számítás befejeződött), ne írjuk felül!
                        continue

                    if not self.is_encoding:
                        current_values = self.tree.item(item_id, 'values')
                        status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                        tags = self.tree.item(item_id, 'tags')
                        if "✓ Kész" not in status and "completed" not in tags and "Ellenőrizendő" not in status and "needs_check" not in tags:
                            orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
                            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                            self.encoding_queue.put(("update", item_id, t('status_nvenc_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                            self.encoding_queue.put(("tag", item_id, "pending"))
                            # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                        break

                    if is_valid is None:
                        with console_redirect(self.svt_logger):
                            print("\n⚠ NVENC 'unexpected end of stream' → SVT-AV1 queue-ba")
                        if output_file.exists() and not DEBUG_MODE:
                            output_file.unlink()
                        svt_task = {
                            'video_path': video_path,
                            'output_file': output_file,
                            'subtitle_files': subtitle_files,
                            'invalid_subtitles': invalid_subtitles,
                            'item_id': item_id,
                            'orig_size_str': orig_size_str,
                            'initial_min_vmaf': self.min_vmaf.get(),
                            'vmaf_step': self.vmaf_step.get(),
                            'max_encoded': self.max_encoded_percent.get(),
                            'resize_enabled': self.resize_enabled.get(),
                            'resize_height': self.resize_height.get(),
                            'audio_compression_enabled': self.audio_compression_enabled.get(),
                            'audio_compression_method': self.audio_compression_method.get(),
                            'target_cq': target_cq,
                            'skip_crf_search': True,
                            'reason': 'manual_reencode_cq_nvenc_failed'
                        }
                        SVT_QUEUE.put(svt_task)
                        current_values = self.tree.item(item_id, 'values')
                        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                        self.encoding_queue.put(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                        self.encoding_queue.put(("tag", item_id, "encoding_svt"))
                        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                        continue
                    elif is_valid:
                        orig_size_mb, new_size_mb, change_percent = self.calculate_file_sizes(video_path, output_file)
                        vmaf_display = final_vmaf if isinstance(final_vmaf, str) else format_localized_number(final_vmaf, decimals=1)
                        orig_size_display = f"{format_localized_number(orig_size_mb, decimals=1)} MB"
                        self.mark_encoding_completed(item_id, f"✓ Kész ({used_encoder})", str(int(final_cq)), vmaf_display, "-", orig_size_display, new_size_mb, change_percent)
                        self._copy_invalid_subtitles(invalid_subtitles, output_file)
                    else:
                        current_values = self.tree.item(item_id, 'values')
                        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                        self.clear_encoding_times(item_id)
                        self.encoding_queue.put(("update", item_id, t('status_failed'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                        self.encoding_queue.put(("tag", item_id, "failed"))
                        self.encoding_queue.put(("progress_bar", 0))
                        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                else:
                    current_values = self.tree.item(item_id, 'values')
                    completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                    if item_id in self.estimated_end_dates:
                        del self.estimated_end_dates[item_id]
                    self.encoding_queue.put(("update", item_id, t('status_failed'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                    self.encoding_queue.put(("tag", item_id, "failed"))
                    self.encoding_queue.put(("progress_bar", 0))
                    # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik

                if self.graceful_stop_requested:
                    break
        finally:
            self.manual_nvenc_active = False
            if hasattr(self, 'root'):
                self.root.after(0, self._on_manual_nvenc_worker_finished)

    def svt_worker(self):
        """Background worker for SVT-AV1 encoding tasks.
        
        Processes videos from the SVT queue, managing encoding, validation,
        and VMAF checks.
        """

        # Alacsony CPU prioritás beállítása
        set_low_priority()
        
        debug_pause.gui_queue = self.encoding_queue
        
        while True:
            if STOP_EVENT.is_set():
                with console_redirect(self.svt_logger):
                    print(f"\n🛑 Azonnali leállítás → SVT-AV1 worker megszakítva\n")
                break
            if self.graceful_stop_requested:
                if SVT_QUEUE.empty():
                    break
                # Ha van még feladat, folytatjuk az aktuális feladat feldolgozását
            try:
                # Nem timeout-tal várunk, amíg NVENC dolgozik, majd hosszabb timeout az elején
                task = SVT_QUEUE.get(timeout=2)
            except queue.Empty:
                # Ha queue üres és NVENC nem dolgozik, vagy leállást kértek, kilépünk
                if STOP_EVENT.is_set() or self.graceful_stop_requested or not self.is_encoding:
                    break
                continue
            
            video_path = task['video_path']
            output_file = task['output_file']
            subtitle_files = task['subtitle_files']
            invalid_subtitles = task.get('invalid_subtitles', [])
            item_id = task['item_id']
            orig_size_str = task['orig_size_str']
            initial_min_vmaf = task['initial_min_vmaf']
            vmaf_step = task['vmaf_step']
            max_encoded = task['max_encoded']
            resize_enabled = task.get('resize_enabled', False)
            resize_height = task.get('resize_height', 1080)
            audio_compression_enabled = task.get('audio_compression_enabled', self.audio_compression_enabled.get())
            audio_compression_method = task.get('audio_compression_method', self.audio_compression_method.get())
            # Ha a combobox értéke fordított szöveg, konvertáljuk
            if audio_compression_method == t('audio_compression_fast'):
                audio_compression_method = 'fast'
            elif audio_compression_method == t('audio_compression_dialogue'):
                audio_compression_method = 'dialogue'
            reason = task['reason']

            if STOP_EVENT.is_set():
                # Thread-safe státusz visszaállítás kérése a főszáltól
                self.encoding_queue.put(("revert_status_if_not_done", item_id, t('status_svt_queue'), orig_size_str))
                
                with console_redirect(self.svt_logger):
                    print(f"\n🛑 Leállítás kérés → SVT-AV1 worker megszakítva\n")
                SVT_QUEUE.task_done()
                break
            
            # Ellenőrizzük, hogy a forrás videó létezik-e
            if not video_path.exists():
                with console_redirect(self.svt_logger):
                    print(f"⚠ Hiba: A forrás videó nem található: {video_path}")
                current_values = self.tree.item(item_id, 'values')
                completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                self.encoding_queue.put(("update", item_id, "✗ Forrás videó hiányzik", "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                self.encoding_queue.put(("tag", item_id, "failed"))
                self.encoding_queue.put(("progress_bar", 0))  # Az érték dinamikusan számolódik
                # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                SVT_QUEUE.task_done()
                continue
            
            with console_redirect(self.svt_logger):
                print(f"\n{'*'*80}")
                print(f"SVT-AV1 FELDOLGOZÁS: {video_path.name}")
                print(f"TELJES ÚTVONAL: {video_path.absolute()}")
                print(f"{'*'*80}\n")
                
                print(f"⏳ Várakozás SVT-AV1 slot-ra...")

            # KRITIKUS: CPU worker lock - biztosítja, hogy csak 1 CPU worker (SVT-AV1 vagy VMAF/PSNR) fusson egyszerre
            # A státusz frissítés a lock-on BELÜL történik, hogy ne legyen race condition
            with CPU_WORKER_LOCK:
                # Leállítás ellenőrzés
                if not self.is_encoding:
                    current_values = self.tree.item(item_id, 'values')
                    status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                    tags = self.tree.item(item_id, 'tags')
                    # Kész vagy ellenőrizendő állapotot nem bolygatunk
                    if "✓ Kész" not in status and "completed" not in tags and "Ellenőrizendő" not in status and "needs_check" not in tags:
                        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                        self.encoding_queue.put(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                        self.encoding_queue.put(("tag", item_id, "pending"))
                        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                        SVT_QUEUE.task_done()
                        continue
                
                # Státusz frissítés: SVT-AV1 queue-ban vár (lock-on belül, hogy ne legyen race condition)
                current_values = self.tree.item(item_id, 'values')
                completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                self.encoding_queue.put(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                
                with console_redirect(self.svt_logger):
                    print(f"✓ SVT-AV1 slot megszerzve, CRF keresés kezdése...\n")

                    def status_callback_svt(msg):
                        self.encoding_queue.put(("status_only", item_id, msg))

                    def progress_callback_svt(msg):
                        self.encoding_queue.put(("progress", item_id, msg))
                        # Becsült befejezési idő számítása a progress alapján (frame szám alapján számolódik)
                        self.update_estimated_end_time_from_progress(item_id, msg)

                    current_values = self.tree.item(item_id, 'values')
                    completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                    # Kezdeti státusz a cél VMAF értékkel
                    localized_vmaf = format_localized_number(initial_min_vmaf, decimals=1)
                    self.encoding_queue.put(("update", item_id, f"SVT-AV1 CRF keresés (VMAF: {localized_vmaf})...", "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                    
                    # Kezdési időpont tárolása
                    self.encoding_start_times[item_id] = time.time()
                    
                    # Ellenőrizzük, hogy skip_crf_search van-e (manuális újrakódolás)
                    skip_crf_search = task.get('skip_crf_search', False)
                    target_cq = task.get('target_cq')
                    
                    if skip_crf_search and target_cq is not None:
                        # Manuális újrakódolás - skip CRF search, használjuk a target_cq-t
                        cq_value_svt = target_cq
                        vmaf_value_svt = task.get('vmaf_value', None)
                        if vmaf_value_svt is None:
                            vmaf_value_svt = "-"  # VMAF nincs analízálva manuális újrakódolásnál
                        with console_redirect(self.svt_logger):
                            print(f"🎬 SVT-AV1 manuális újrakódolás (CRF keresés kihagyva): {video_path.name}")
                            print(f"   Cél CRF: {target_cq}")
                    else:
                        # Normál folyamat - CRF keresés
                        # FONTOS: Ellenőrizzük, hogy ugyanaz a video_path kerül használatra a CRF kereséshez és a kódoláshoz
                        video_path_abs_svt = video_path.absolute()
                        try:
                            with console_redirect(self.svt_logger):
                                print(f"🎬 SVT-AV1 CRF keresés indul: {video_path.name}")
                                print(f"🔍 CRF keresés fájl ellenőrzés (teljes útvonal): {video_path_abs_svt}")
                                cq_result_svt = run_crf_search(video_path, encoder='svt-av1', initial_min_vmaf=initial_min_vmaf, vmaf_step=vmaf_step, max_encoded_percent=max_encoded, progress_callback=status_callback_svt, logger=self.svt_logger, stop_event=STOP_EVENT, svt_preset=self.svt_preset.get())
                                print(f"✓ SVT-AV1 CRF keresés kész: {cq_result_svt}")
                        except FileNotFoundError as e:
                            # Ab-av1.exe nem található - végzetes hiba
                            error_msg = f"VÉGZETES HIBA: Az ab-av1.exe nem található vagy nem indítható!\n\nHiba: {e}\n\nA program nem tudja elindítani az ab-av1.exe-t, ezért a CRF keresés nem lehetséges.\n\nEllenőrizd, hogy az ab-av1.exe létezik-e a megadott útvonalon, vagy állítsd be a helyes útvonalat a beállításokban."
                            with console_redirect(self.svt_logger):
                                print(f"\n{'='*80}")
                                print(f"⚠⚠⚠ VÉGZETES HIBA ⚠⚠⚠")
                                print(f"{'='*80}")
                                print(error_msg)
                                print(f"{'='*80}\n")
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"\n{'='*80}\n")
                                    LOG_WRITER.write(f"⚠⚠⚠ VÉGZETES HIBA ⚠⚠⚠\n")
                                    LOG_WRITER.write(f"{'='*80}\n")
                                    LOG_WRITER.write(f"{error_msg}\n")
                                    LOG_WRITER.write(f"{'='*80}\n\n")
                                    LOG_WRITER.flush()
                                except Exception:
                                    pass
                            # Azonnali leállítás
                            STOP_EVENT.set()
                            self.graceful_stop_requested = True
                            # MessageBox hibaüzenet (GUI thread-ben)
                            self.root.after(0, lambda: messagebox.showerror(
                                "VÉGZETES HIBA",
                                error_msg
                            ))
                            # Várunk egy kicsit, hogy a MessageBox megjelenjen
                            time.sleep(0.5)
                            # Visszaállítjuk a státuszt
                            current_values = self.tree.item(item_id, 'values')
                            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                            self.encoding_queue.put(("update", item_id, "✗ Ab-av1.exe nem található", "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                            self.encoding_queue.put(("tag", item_id, "failed"))
                            SVT_QUEUE.task_done()
                            continue
                        except NoSuitableCRFFound:
                            # Nincs megfelelő CRF (VMAF >= 85 ÉS fájl <= 75%) → egyszerű másolás
                            with console_redirect(self.svt_logger):
                                print(f"\n⚠ Nincs megfelelő CRF (VMAF >= 85.0 ÉS fájl <= 75%)")
                                print(f"   → Videó másolása feliratokkal együtt átkódolás nélkül\n")
                            
                            # Másolás
                            copy_success = copy_video_and_subtitles(video_path, output_file)
                            
                            if copy_success:
                                # Sikeres másolás - kész státusz
                                orig_size_mb = video_path.stat().st_size / (1024**2)
                                new_size_mb = output_file.stat().st_size / (1024**2)
                                orig_size_str = f"{format_localized_number(orig_size_mb, decimals=1)} MB"
                                new_size_str = f"{format_localized_number(new_size_mb, decimals=1)} MB"
                                completed_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                
                                # Becsült befejezési idő törlése
                                if item_id in self.estimated_end_dates:
                                    del self.estimated_end_dates[item_id]
                                
                                self.encoding_queue.put(("update", item_id, t('status_completed_copy'), "-", "-", "-", "-", "100%", orig_size_str, new_size_str, "0%", completed_date))
                                self.encoding_queue.put(("tag", item_id, "completed"))
                                self.encoding_queue.put(("progress_bar", 0))  # Az érték dinamikusan számolódik
                                
                                # Adatbázis frissítése másolás befejezése után
                                if video_path:
                                    def update_db_after_copy():
                                        try:
                                            self.update_single_video_in_db(
                                                video_path, item_id, t('status_completed_copy'), 
                                                "-", "-", "-", orig_size_str, 
                                                new_size_mb, 0.0, completed_date
                                            )
                                        except Exception as e:
                                            if LOG_WRITER:
                                                try:
                                                    LOG_WRITER.write(f"⚠ [copy] Adatbázis frissítés hiba: {e} | video: {video_path}\n")
                                                    LOG_WRITER.flush()
                                                except Exception:
                                                    pass
                                    
                                    db_thread = threading.Thread(target=update_db_after_copy, daemon=True)
                                    db_thread.start()
                                
                                with console_redirect(self.svt_logger):
                                    print(f"✓ Videó sikeresen másolva: {output_file.name}\n")
                            else:
                                # Másolás sikertelen (pl. már létezik a célhelyen) - skip
                                completed_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                
                                # Becsült befejezési idő törlése
                                if item_id in self.estimated_end_dates:
                                    del self.estimated_end_dates[item_id]
                                
                                self.encoding_queue.put(("update", item_id, t('status_completed_exists'), "-", "-", "-", "-", "100%", orig_size_str, "-", "-", completed_date))
                                self.encoding_queue.put(("tag", item_id, "completed"))
                                self.encoding_queue.put(("progress_bar", 0))  # Az érték dinamikusan számolódik
                                
                                # Adatbázis frissítése "már létezik" esetén
                                if video_path:
                                    # Próbáljuk meg meghatározni az output fájl méretét
                                    new_size_mb = None
                                    if output_file and output_file.exists():
                                        try:
                                            new_size_mb = output_file.stat().st_size / (1024**2)
                                        except (OSError, PermissionError):
                                            pass
                                    
                                    def update_db_after_exists():
                                        try:
                                            self.update_single_video_in_db(
                                                video_path, item_id, t('status_completed_exists'), 
                                                "-", "-", "-", orig_size_str, 
                                                new_size_mb, None, completed_date
                                            )
                                        except Exception as e:
                                            if LOG_WRITER:
                                                try:
                                                    LOG_WRITER.write(f"⚠ [exists] Adatbázis frissítés hiba: {e} | video: {video_path}\n")
                                                    LOG_WRITER.flush()
                                                except Exception:
                                                    pass
                                    
                                    db_thread = threading.Thread(target=update_db_after_exists, daemon=True)
                                    db_thread.start()
                                
                                with console_redirect(self.svt_logger):
                                    print(f"⚠ Videó már létezik a célhelyen, átugrás\n")
                            
                            SVT_QUEUE.task_done()
                            continue
                        except EncodingStopped:
                            current_values = self.tree.item(item_id, 'values')
                            status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                            tags = self.tree.item(item_id, 'tags')
                            # Kész vagy ellenőrizendő állapotot nem bolygatunk
                            if "✓ Kész" not in status and "completed" not in tags and "Ellenőrizendő" not in status and "needs_check" not in tags:
                                completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                                self.encoding_queue.put(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                                self.encoding_queue.put(("tag", item_id, "pending"))
                                # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                            with console_redirect(self.svt_logger):
                                print(f"\n🛑 Leállítás kérés → SVT-AV1 worker megszakítva\n")
                            SVT_QUEUE.task_done()
                            return
                        
                        # Leállítás ellenőrzés CRF keresés után
                        if not self.is_encoding:
                            current_values = self.tree.item(item_id, 'values')
                            status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                            tags = self.tree.item(item_id, 'tags')
                            # Kész vagy ellenőrizendő állapotot nem bolygatunk
                            if "✓ Kész" not in status and "completed" not in tags and "Ellenőrizendő" not in status and "needs_check" not in tags:
                                completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                                self.encoding_queue.put(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                                self.encoding_queue.put(("tag", item_id, "pending"))
                                # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                            SVT_QUEUE.task_done()
                            continue
                        
                        cq_value_svt, vmaf_value_svt = cq_result_svt
                    current_values = self.tree.item(item_id, 'values')
                    completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                    vmaf_display = vmaf_value_svt if isinstance(vmaf_value_svt, str) else format_localized_number(vmaf_value_svt, decimals=1)
                    self.encoding_queue.put(("update", item_id, f"SVT-AV1 kódolás ({reason})...", str(int(cq_value_svt)), vmaf_display, "-", "-", orig_size_str, "-", "-", completed_date))
                    
                    # Ha még nincs kezdési időpont (pl. ha CRF keresés nélkül kezdődik), akkor most tároljuk
                    self.encoding_start_times[item_id] = time.time()
                    
                    # SVT kódolás SVT konzolra irányítva
                    # FONTOS: Ellenőrizzük, hogy ugyanaz a video_path kerül használatra a kódoláshoz, mint a CRF kereséshez
                    video_path_abs_check_svt = video_path.absolute()
                    if not skip_crf_search:
                        if video_path_abs_check_svt != video_path_abs_svt:
                            error_msg = f"VÉGZETES HIBA: A video_path megváltozott a CRF keresés és a kódolás között!\n\nCRF keresés fájl: {video_path_abs_svt}\nKódolás fájl: {video_path_abs_check_svt}\n\nEz azt jelenti, hogy a CRF keresés egy fájlhoz történt, de a kódolás másik fájlhoz kezdődött. Ez kritikus hiba!\n\nA program azonnal leáll."
                            with console_redirect(self.svt_logger):
                                print(f"\n{'='*80}")
                                print(f"⚠⚠⚠ VÉGZETES HIBA ⚠⚠⚠")
                                print(f"{'='*80}")
                                print(error_msg)
                                print(f"{'='*80}\n")
                            # Log fájlba is írjuk
                            if LOG_WRITER:
                                try:
                                    LOG_WRITER.write(f"\n{'='*80}\n")
                                    LOG_WRITER.write(f"⚠⚠⚠ VÉGZETES HIBA ⚠⚠⚠\n")
                                    LOG_WRITER.write(f"{'='*80}\n")
                                    LOG_WRITER.write(f"{error_msg}\n")
                                    LOG_WRITER.write(f"{'='*80}\n\n")
                                    LOG_WRITER.flush()
                                except Exception:
                                    pass
                            # Azonnali leállítás
                            STOP_EVENT.set()
                            self.graceful_stop_requested = True
                            # MessageBox hibaüzenet (GUI thread-ben)
                            self.root.after(0, lambda: messagebox.showerror(
                                "VÉGZETES HIBA",
                                error_msg
                            ))
                            # Várunk egy kicsit, hogy a MessageBox megjelenjen
                            time.sleep(0.5)
                            raise ValueError(error_msg)
                    try:
                        with console_redirect(self.svt_logger):
                            if skip_crf_search and target_cq is not None:
                                # Manuális újrakódolás - encode_single_attempt használata (skip encode_video belső CRF keresését)
                                print(f"🔍 Kódolás fájl ellenőrzés (skip_crf_search, teljes útvonal): {video_path_abs_check_svt}")
                                print(f"🎬 SVT-AV1 kódolás kezdése: {video_path.name}")
                                print(f"   Teljes útvonal: {video_path_abs_check_svt}")
                                print(f"   Cél fájl: {output_file.absolute()}")
                                success_svt = encode_single_attempt(video_path, output_file, target_cq, subtitle_files, 'svt-av1', progress_callback_svt, stop_event=STOP_EVENT, vmaf_value=vmaf_value_svt, resize_enabled=resize_enabled, resize_height=resize_height, audio_compression_enabled=audio_compression_enabled, audio_compression_method=audio_compression_method, svt_preset=self.svt_preset.get(), logger=self.svt_logger)
                            else:
                                # Normál folyamat - encode_video használata (CRF keresés benne van)
                                print(f"🔍 Kódolás fájl ellenőrzés (teljes útvonal): {video_path_abs_check_svt}")
                                print(f"🎬 SVT-AV1 kódolás kezdése: {video_path.name}")
                                print(f"   Teljes útvonal: {video_path_abs_check_svt}")
                                print(f"   Cél fájl: {output_file.absolute()}")
                                success_svt = encode_video(video_path, output_file, cq_value_svt, subtitle_files, 'svt-av1', progress_callback_svt, initial_min_vmaf, vmaf_step, max_encoded, stop_event=STOP_EVENT, vmaf_value=vmaf_value_svt, resize_enabled=resize_enabled, resize_height=resize_height, audio_compression_enabled=audio_compression_enabled, audio_compression_method=audio_compression_method, svt_preset=self.svt_preset.get(), logger=self.svt_logger)
                    except EncodingStopped:
                        current_values = self.tree.item(item_id, 'values')
                        status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                        tags = self.tree.item(item_id, 'tags')
                        # Kész vagy ellenőrizendő állapotot nem bolygatunk
                        if "✓ Kész" not in status and "completed" not in tags and "Ellenőrizendő" not in status and "needs_check" not in tags:
                            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                            self.encoding_queue.put(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                            self.encoding_queue.put(("tag", item_id, "pending"))
                            # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                        with console_redirect(self.svt_logger):
                            print(f"\n🛑 Leállítás kérés → SVT-AV1 worker megszakítva\n")
                        SVT_QUEUE.task_done()
                        return
                    
                    # Leállítás ellenőrzés kódolás után
                    if not self.is_encoding:
                        current_values = self.tree.item(item_id, 'values')
                        status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                        tags = self.tree.item(item_id, 'tags')
                        # Kész vagy ellenőrizendő állapotot nem bolygatunk
                        if "✓ Kész" not in status and "completed" not in tags and "Ellenőrizendő" not in status and "needs_check" not in tags:
                            self.encoding_queue.put(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", ""))
                            self.encoding_queue.put(("tag", item_id, "pending"))
                            # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                        SVT_QUEUE.task_done()
                        continue
                    
                    if success_svt:
                        # KRITIKUS: Ellenőrizzük, hogy a videó már "Kész" állapotban van-e (pl. VMAF/PSNR számítás után)
                        # Ha igen, ne indítsuk újra a validálást!
                        current_values = self.tree.item(item_id, 'values')
                        status_before_validation = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                        tags_before_validation = self.tree.item(item_id, 'tags')
                        is_already_completed = (
                            "✓ Kész" in status_before_validation or 
                            "completed" in tags_before_validation or 
                            "Kész" in status_before_validation
                        )
                        
                        if is_already_completed:
                            # A videó már kész (pl. VMAF/PSNR számítás után), ne indítsuk újra a validálást!
                            SVT_QUEUE.task_done()
                            continue
                        
                        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                        vmaf_display = vmaf_value_svt if isinstance(vmaf_value_svt, str) else format_localized_number(vmaf_value_svt, decimals=1)
                        self.encoding_queue.put(("update", item_id, f"SVT-AV1 validálás ({reason})...", str(int(cq_value_svt)), vmaf_display, "-", "100%", orig_size_str, "-", "-", completed_date))
                        
                        # Leállítás ellenőrzés validálás előtt
                        if not self.is_encoding:
                            current_values = self.tree.item(item_id, 'values')
                            status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                            tags = self.tree.item(item_id, 'tags')
                            # Kész vagy ellenőrizendő állapotot nem bolygatunk
                            if "✓ Kész" not in status and "completed" not in tags and "Ellenőrizendő" not in status and "needs_check" not in tags:
                                completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                                self.encoding_queue.put(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                                self.encoding_queue.put(("tag", item_id, "pending"))
                                # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                            SVT_QUEUE.task_done()
                            continue
                        
                        # Validálás SVT konzolra irányítva
                        try:
                            with console_redirect(self.svt_logger):
                                is_valid = validate_encoded_video_vlc(output_file, encoder='svt-av1', stop_event=STOP_EVENT, source_path=video_path)
                        except EncodingStopped:
                            current_values = self.tree.item(item_id, 'values')
                            status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                            tags = self.tree.item(item_id, 'tags')
                            # Kész vagy ellenőrizendő állapotot nem bolygatunk
                            if "✓ Kész" not in status and "completed" not in tags and "Ellenőrizendő" not in status and "needs_check" not in tags:
                                completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                                self.encoding_queue.put(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                                self.encoding_queue.put(("tag", item_id, "pending"))
                                # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                            with console_redirect(self.svt_logger):
                                print(f"\n🛑 Leállítás kérés → SVT-AV1 worker megszakítva\n")
                            SVT_QUEUE.task_done()
                            return
                        
                        # KRITIKUS: Újraellenőrizzük a validálás után is, hogy a videó már "Kész" állapotban van-e
                        # (lehet, hogy közben VMAF/PSNR számítás befejeződött)
                        current_values = self.tree.item(item_id, 'values')
                        status_after_validation = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                        tags_after_validation = self.tree.item(item_id, 'tags')
                        is_now_completed = (
                            "✓ Kész" in status_after_validation or 
                            "completed" in tags_after_validation or 
                            "Kész" in status_after_validation
                        )
                        
                        if is_now_completed:
                            # A videó közben kész lett (pl. VMAF/PSNR számítás befejeződött), ne írjuk felül!
                            SVT_QUEUE.task_done()
                            continue
                        
                        # Leállítás ellenőrzés validálás után
                        if not self.is_encoding:
                            current_values = self.tree.item(item_id, 'values')
                            status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                            tags = self.tree.item(item_id, 'tags')
                            # Kész vagy ellenőrizendő állapotot nem bolygatunk
                            if "✓ Kész" not in status and "completed" not in tags and "Ellenőrizendő" not in status and "needs_check" not in tags:
                                completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                                self.encoding_queue.put(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                                self.encoding_queue.put(("tag", item_id, "pending"))
                                # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                            SVT_QUEUE.task_done()
                            continue
                        
                        if is_valid:
                            orig_size_mb, new_size_mb, change_percent = self.calculate_file_sizes(video_path, output_file)
                            vmaf_display = vmaf_value_svt if isinstance(vmaf_value_svt, str) else format_localized_number(vmaf_value_svt, decimals=1)
                            orig_size_display = f"{format_localized_number(orig_size_mb, decimals=1)} MB"
                            self.mark_encoding_completed(item_id, t('status_completed_svt'), str(int(cq_value_svt)), vmaf_display, "-", orig_size_display, new_size_mb, change_percent)
                            self._copy_invalid_subtitles(invalid_subtitles, output_file)
                            
                            with console_redirect(self.svt_logger):
                                orig_size_str_log = format_localized_number(orig_size_mb, decimals=1)
                                new_size_str_log = format_localized_number(new_size_mb, decimals=1)
                                change_percent_str_log = format_localized_number(change_percent, decimals=2, show_sign=True)
                                print(f"\n✓ SVT-AV1 kódolás sikeres: {video_path.name}")
                                print(f"  {orig_size_str_log}MB → {new_size_str_log}MB ({change_percent_str_log}%)\n")
                        elif not is_valid and output_file.exists():
                            orig_size_mb = video_path.stat().st_size / (1024**2)
                            new_size_mb = output_file.stat().st_size / (1024**2)
                            change_percent = ((new_size_mb - orig_size_mb) / orig_size_mb) * 100 if orig_size_mb > 0 else 0
                            
                            current_values = self.tree.item(item_id, 'values')
                            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                            vmaf_display = vmaf_value_svt if isinstance(vmaf_value_svt, str) else format_localized_number(vmaf_value_svt, decimals=1)
                            new_size_str_check = f"{format_localized_number(new_size_mb, decimals=1)} MB"
                            change_percent_str_check = f"{format_localized_number(change_percent, decimals=2, show_sign=True)}%"
                            self.encoding_queue.put(("update", item_id, "⚠ Ellenőrizendő (SVT)", str(int(cq_value_svt)), vmaf_display, "-", "100%", orig_size_str, new_size_str_check, change_percent_str_check, completed_date))
                            self.encoding_queue.put(("tag", item_id, "needs_check"))
                            self.encoding_queue.put(("progress_bar", 0))  # Az érték dinamikusan számolódik
                            
                            with console_redirect(self.svt_logger):
                                print(f"\n⚠ SVT-AV1 validáció sikertelen, ellenőrizendő: {video_path.name}\n")
                        else:
                            current_values = self.tree.item(item_id, 'values')
                            completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                            # Becsült befejezési idő törlése
                            if item_id in self.estimated_end_dates:
                                del self.estimated_end_dates[item_id]
                            
                            self.encoding_queue.put(("update", item_id, t('status_failed'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                            self.encoding_queue.put(("tag", item_id, "failed"))
                            self.encoding_queue.put(("progress_bar", 0))  # Az érték dinamikusan számolódik
                            
                            if output_file.exists() and not DEBUG_MODE:
                                output_file.unlink()
                            
                            with console_redirect(self.svt_logger):
                                print(f"\n✗ SVT-AV1 kódolás sikertelen: {video_path.name}\n")
                    else:
                        # ========================================================================
                        # COPY FALLBACK: Encoding sikertelen → Változatlan másolás
                        # ========================================================================
                        with console_redirect(self.svt_logger):
                            print(f"\n⚠ SVT-AV1 kódolás sikertelen: {video_path.name}")
                            print(f"   → Változatlan másolás (eredeti kiterjesztés megtartva)...\n")
                        
                        # Generate copy destination with ORIGINAL extension
                        copy_dest = get_copy_filename(video_path, self.source_path, self.dest_path)
                        
                        # Perform copy with validated subtitles
                        copy_success = copy_video_fallback(
                            video_path,
                            copy_dest,
                            subtitle_files,  # Valid subtitles only
                            logger=self.svt_logger
                        )
                        
                        if copy_success:
                            # Calculate sizes
                            try:
                                orig_size_mb, new_size_mb, change_percent = self.calculate_file_sizes(
                                    video_path, copy_dest
                                )
                                orig_size_display = f"{format_localized_number(orig_size_mb, decimals=1)} MB"
                                new_size_display = f"{format_localized_number(new_size_mb, decimals=1)} MB"
                                change_display = "0%"  # No change
                            except Exception as e:
                                with console_redirect(self.svt_logger):
                                    print(f"⚠ Méretszámítás hiba: {e}")
                                orig_size_mb = video_path.stat().st_size / (1024**2)
                                new_size_mb = orig_size_mb
                                change_percent = 0
                                orig_size_display = f"{format_localized_number(orig_size_mb, decimals=1)} MB"
                                new_size_display = orig_size_display
                                change_display = "0%"
                            
                            # Mark as completed (copied) in tree
                            completed_date_copy = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            self.encoding_queue.put((
                                "update", 
                                item_id, 
                                t('status_completed_copy'),  # "✓ Kész (másolva)"
                                "-",  # No CQ
                                "-",  # No VMAF
                                "-",  # No PSNR
                                "100%",  # Progress
                                orig_size_display,
                                new_size_display,
                                change_display,
                                completed_date_copy
                            ))
                            self.encoding_queue.put(("tag", item_id, "completed"))
                            self.encoding_queue.put(("progress_bar", 0))
                            
                            # Update video_to_output mapping (CRITICAL!)
                            self.video_to_output[video_path] = copy_dest
                            
                            # Copy invalid subtitles too
                            try:
                                self._copy_invalid_subtitles(invalid_subtitles, copy_dest)
                            except Exception as e:
                                with console_redirect(self.svt_logger):
                                    print(f"⚠ Érvénytelen feliratok másolása hiba: {e}")
                            
                            # Database update in background thread
                            def update_db_after_copy():
                                try:
                                    self.update_single_video_in_db(
                                        video_path, item_id, t('status_completed_copy'),
                                        "-", "-", "-",
                                        orig_size_display, new_size_mb, change_percent, completed_date_copy
                                    )
                                except Exception as e:
                                    if LOG_WRITER:
                                        try:
                                            LOG_WRITER.write(f"⚠ [copy] Adatbázis frissítés hiba: {e} | video: {video_path}\n")
                                            LOG_WRITER.flush()
                                        except Exception:
                                            pass
                            
                            db_thread = threading.Thread(target=update_db_after_copy, daemon=True)
                            db_thread.start()
                            
                            with console_redirect(self.svt_logger):
                                orig_mb_str = format_localized_number(orig_size_mb, decimals=1)
                                print(f"\n✓ Videó változatlan másolva: {video_path.name}")
                                print(f"  {orig_mb_str} MB (nincs méretváltozás)\n")
                        else:
                            # Copy also failed - mark as failed
                            completed_date = ""
                            self.encoding_queue.put((
                                "update",
                                item_id,
                                "✗ Hiba (másolás sikertelen)",
                                "-", "-", "-", "-",
                                orig_size_str, "-", "-", completed_date
                            ))
                            self.encoding_queue.put(("tag", item_id, "failed"))
                            self.encoding_queue.put(("progress_bar", 0))
                            
                            with console_redirect(self.svt_logger):
                                print(f"\n✗ Másolás is sikertelen: {video_path.name}\n")
                    
                    with console_redirect(self.svt_logger):
                        print(f"✓ SVT-AV1 slot felszabadítva\n")
            
            SVT_QUEUE.task_done()
        
        if self.graceful_stop_requested and not STOP_EVENT.is_set():
            with console_redirect(self.svt_logger):
                print(f"\n🟡 Leállítás kérése → SVT-AV1 worker megszakítva\n")

        with console_redirect(self.svt_logger):
            print(f"\n{'#'*80}\n### SVT-AV1 WORKER BEFEJEZVE ###\n{'#'*80}\n")

    def nvenc_worker(self, worker_index):
        """Background worker for NVENC encoding tasks.
        
        Args:
            worker_index: Index of the worker thread (0-based).
            
        Processes videos from the NVENC queue, managing encoding, validation,
        and VMAF checks.
        """

        # Worker index beállítása a logger-hez
        # Modulo használata, hogy ha több worker van, mint logger, akkor is helyesen működjön
        # A logger objektumot modulo alapján választjuk (logger_idx = worker_index % len(nvenc_loggers))
        # A logger objektum logger_index-e nem változik, így minden worker a saját logger objektumához
        # tartozó log fájlba és konzolba ír (logger_index alapján)
        if len(self.nvenc_loggers) > 0:
            logger_idx = worker_index % len(self.nvenc_loggers)
            nvenc_logger = self.nvenc_loggers[logger_idx]
            # A logger worker_index-ét a tényleges worker_index-re állítjuk (queue üzenetekhez)
            # A log fájl kiválasztásánál a logger_index-et használjuk (ami nem változik),
            # így minden worker a saját logger objektumához tartozó log fájlba írjon
            nvenc_logger.set_worker_index(worker_index)
        else:
            nvenc_logger = None
        
        debug_pause.gui_queue = self.encoding_queue
        
        # Aktív videó hozzáadása
        with self.nvenc_selection_lock:
            self.nvenc_active_videos.add(worker_index)
        
        current_video_path = None  # Tároljuk az aktuális videót, hogy a finally blokkban eltávolíthassuk
        try:
            while True:
                if STOP_EVENT.is_set():
                    with console_redirect(nvenc_logger):
                        print(f"\n🛑 Azonnali leállítás → NVENC worker #{worker_index + 1} megszakítva\n")
                    break
                if self.graceful_stop_requested:
                    if NVENC_QUEUE.empty():
                        break
                    # Ha van még feladat, folytatjuk az aktuális feladat feldolgozását
                try:
                    task = NVENC_QUEUE.get(timeout=2)
                except queue.Empty:
                    # Ha queue üres és leállást kértek, kilépünk
                    if STOP_EVENT.is_set() or self.graceful_stop_requested or not self.is_encoding:
                        break
                    continue
                
                video_path = task['video_path']
                current_video_path = video_path  # Tároljuk az aktuális videót
                output_file = task['output_file']
                subtitle_files = task['subtitle_files']
                invalid_subtitles = task.get('invalid_subtitles', [])
                item_id = task['item_id']
                orig_size_str = task['orig_size_str']
                initial_min_vmaf = task['initial_min_vmaf']
                vmaf_step = task['vmaf_step']
                max_encoded = task['max_encoded']
                resize_enabled = task.get('resize_enabled', False)
                resize_height = task.get('resize_height', 1080)
                audio_compression_enabled = task.get('audio_compression_enabled', self.audio_compression_enabled.get())
                audio_compression_method = task.get('audio_compression_method', self.audio_compression_method.get())
                # Ha a combobox értéke fordított szöveg, konvertáljuk
                if audio_compression_method == t('audio_compression_fast'):
                    audio_compression_method = 'fast'
                elif audio_compression_method == t('audio_compression_dialogue'):
                    audio_compression_method = 'dialogue'
                
                # Helper függvény a task_done és processing set eltávolításához
                def finish_nvenc_task():
                    nonlocal current_video_path
                    with self.nvenc_selection_lock:
                        if current_video_path:
                            self.nvenc_processing_videos.discard(current_video_path)
                            current_video_path = None
                    NVENC_QUEUE.task_done()
                
                if STOP_EVENT.is_set():
                    # Thread-safe státusz visszaállítás kérése a főszáltól
                    self.encoding_queue.put(("revert_status_if_not_done", item_id, t('status_nvenc_queue'), orig_size_str))
                    with console_redirect(nvenc_logger):
                        print(f"\n🛑 Leállítás kérés → NVENC worker #{worker_index + 1} megszakítva\n")
                    finish_nvenc_task()
                    break
                
                # Ellenőrizzük, hogy a forrás videó létezik-e
                if not video_path.exists():
                    with console_redirect(nvenc_logger):
                        print(f"⚠ Hiba: A forrás videó nem található: {video_path}")
                    current_values = self.tree.item(item_id, 'values')
                    completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                    self.encoding_queue.put(("update", item_id, "✗ Forrás videó hiányzik", "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                    self.encoding_queue.put(("tag", item_id, "failed"))
                    self.encoding_queue.put(("progress_bar", 0))
                    # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                    finish_nvenc_task()
                    continue
                
                with console_redirect(nvenc_logger):
                    print(f"\n{'*'*80}")
                    print(f"NVENC FELDOLGOZÁS (Worker #{worker_index + 1}): {video_path.name}")
                    print(f"TELJES ÚTVONAL: {video_path.absolute()}")
                    print(f"{'*'*80}\n")
                
                current_values = self.tree.item(item_id, 'values')
                completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                
                # Leállítás ellenőrzés
                if not self.is_encoding:
                    current_values = self.tree.item(item_id, 'values')
                    status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                    tags = self.tree.item(item_id, 'tags')
                    # Kész vagy ellenőrizendő állapotot nem bolygatunk
                    if "✓ Kész" not in status and "completed" not in tags and "Ellenőrizendő" not in status and "needs_check" not in tags:
                        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                        self.encoding_queue.put(("update", item_id, t('status_nvenc_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                        self.encoding_queue.put(("tag", item_id, "pending"))
                        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                    finish_nvenc_task()
                    continue
                
                with console_redirect(nvenc_logger):
                    print(f"✓ NVENC worker #{worker_index + 1} slot megszerzve, CRF keresés kezdése...\n")
                
                def status_callback(msg):
                    self.encoding_queue.put(("status_only", item_id, msg))
                
                def progress_callback(msg):
                    self.encoding_queue.put(("progress", item_id, msg))
                    self.update_estimated_end_time_from_progress(item_id, msg)
                
                current_values = self.tree.item(item_id, 'values')
                completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                localized_vmaf = format_localized_number(initial_min_vmaf, decimals=1)
                self.encoding_queue.put(("update", item_id, f"NVENC CRF keresés (VMAF: {localized_vmaf})...", "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                # Amikor ténylegesen elkezd dolgozni, akkor encoding_nvenc tag (narancs)
                self.encoding_queue.put(("tag", item_id, "encoding_nvenc"))
                
                # Kezdési időpont tárolása
                self.encoding_start_times[item_id] = time.time()
                
                # CRF keresés
                # FONTOS: Ellenőrizzük, hogy ugyanaz a video_path kerül használatra a CRF kereséshez és a kódoláshoz
                video_path_abs = video_path.absolute()
                try:
                    with console_redirect(nvenc_logger):
                        print(f"🎬 NVENC CRF keresés indul: {video_path.name}")
                        print(f"🔍 CRF keresés fájl ellenőrzés (teljes útvonal): {video_path_abs}")
                        cq_result_nvenc = run_crf_search(video_path, encoder='av1_nvenc', initial_min_vmaf=initial_min_vmaf, vmaf_step=vmaf_step, max_encoded_percent=max_encoded, progress_callback=status_callback, logger=nvenc_logger, stop_event=STOP_EVENT)
                        print(f"✓ NVENC CRF keresés kész: {cq_result_nvenc}")
                except FileNotFoundError as e:
                    # Ab-av1.exe nem található - végzetes hiba
                    error_msg = f"VÉGZETES HIBA: Az ab-av1.exe nem található vagy nem indítható!\n\nHiba: {e}\n\nA program nem tudja elindítani az ab-av1.exe-t, ezért a CRF keresés nem lehetséges.\n\nEllenőrizd, hogy az ab-av1.exe létezik-e a megadott útvonalon, vagy állítsd be a helyes útvonalat a beállításokban."
                    with console_redirect(nvenc_logger):
                        print(f"\n{'='*80}")
                        print(f"⚠⚠⚠ VÉGZETES HIBA ⚠⚠⚠")
                        print(f"{'='*80}")
                        print(error_msg)
                        print(f"{'='*80}\n")
                    if LOG_WRITER:
                        try:
                            LOG_WRITER.write(f"\n{'='*80}\n")
                            LOG_WRITER.write(f"⚠⚠⚠ VÉGZETES HIBA ⚠⚠⚠\n")
                            LOG_WRITER.write(f"{'='*80}\n")
                            LOG_WRITER.write(f"{error_msg}\n")
                            LOG_WRITER.write(f"{'='*80}\n\n")
                            LOG_WRITER.flush()
                        except Exception:
                            pass
                    # Azonnali leállítás
                    STOP_EVENT.set()
                    self.graceful_stop_requested = True
                    # MessageBox hibaüzenet (GUI thread-ben)
                    self.root.after(0, lambda: messagebox.showerror(
                        "VÉGZETES HIBA",
                        error_msg
                    ))
                    # Várunk egy kicsit, hogy a MessageBox megjelenjen
                    time.sleep(0.5)
                    # Visszaállítjuk a státuszt
                    current_values = self.tree.item(item_id, 'values')
                    completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                    self.encoding_queue.put(("update", item_id, "✗ Ab-av1.exe nem található", "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                    self.encoding_queue.put(("tag", item_id, "failed"))
                    finish_nvenc_task()
                    continue
                except EncodingStopped:
                    current_values = self.tree.item(item_id, 'values')
                    status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                    tags = self.tree.item(item_id, 'tags')
                    # Kész vagy ellenőrizendő állapotot nem bolygatunk
                    if "✓ Kész" not in status and "completed" not in tags and "Ellenőrizendő" not in status and "needs_check" not in tags:
                        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                        self.encoding_queue.put(("update", item_id, t('status_nvenc_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                        self.encoding_queue.put(("tag", item_id, "pending"))
                        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                    finish_nvenc_task()
                    continue
                
                if len(cq_result_nvenc) == 3 and cq_result_nvenc[2]:
                    # VMAF fallback elfogyott → SVT queue-ba
                    cq_value_nvenc, vmaf_value_nvenc, fallback_exhausted = cq_result_nvenc
                    with console_redirect(self.svt_logger):
                        print(f"\n⚠ NVENC VMAF fallback elfogyott → automatikusan SVT-AV1 queue-ba helyezés")
                    
                    if output_file.exists() and not DEBUG_MODE:
                        output_file.unlink()
                    
                    svt_task = {
                        'video_path': video_path,
                        'output_file': output_file,
                        'subtitle_files': subtitle_files,
                        'invalid_subtitles': invalid_subtitles,
                        'item_id': item_id,
                        'orig_size_str': orig_size_str,
                        'initial_min_vmaf': initial_min_vmaf,
                        'vmaf_step': vmaf_step,
                        'max_encoded': max_encoded,
                        'resize_enabled': resize_enabled,
                        'resize_height': resize_height,
                        'audio_compression_enabled': audio_compression_enabled,
                        'audio_compression_method': audio_compression_method,
                        'reason': 'nvenc_fallback_exhausted'
                    }
                    SVT_QUEUE.put(svt_task)
                    current_values = self.tree.item(item_id, 'values')
                    completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                    self.encoding_queue.put(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                    self.encoding_queue.put(("tag", item_id, "encoding_svt"))
                    # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                    finish_nvenc_task()
                    continue
                else:
                    cq_value_nvenc, vmaf_value_nvenc = cq_result_nvenc
                
                current_values = self.tree.item(item_id, 'values')
                completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                vmaf_display = format_localized_number(vmaf_value_nvenc, decimals=1) if vmaf_value_nvenc is not None else "-"
                self.encoding_queue.put(("update", item_id, "NVENC kódolás...", str(int(cq_value_nvenc)), vmaf_display, "-", "-", orig_size_str, "-", "-", completed_date))
                self.encoding_queue.put(("tag", item_id, "encoding_nvenc"))
                
                # Kezdési időpont tárolása
                self.encoding_start_times[item_id] = time.time()
                
                # NVENC kódolás
                # FONTOS: Ellenőrizzük, hogy ugyanaz a video_path kerül használatra a kódoláshoz, mint a CRF kereséshez
                video_path_abs_check = video_path.absolute()
                if video_path_abs_check != video_path_abs:
                    error_msg = f"VÉGZETES HIBA: A video_path megváltozott a CRF keresés és a kódolás között!\n\nCRF keresés fájl: {video_path_abs}\nKódolás fájl: {video_path_abs_check}\n\nEz azt jelenti, hogy a CRF keresés egy fájlhoz történt, de a kódolás másik fájlhoz kezdődött. Ez kritikus hiba!\n\nA program azonnal leáll."
                    with console_redirect(nvenc_logger):
                        print(f"\n{'='*80}")
                        print(f"⚠⚠⚠ VÉGZETES HIBA ⚠⚠⚠")
                        print(f"{'='*80}")
                        print(error_msg)
                        print(f"{'='*80}\n")
                    # Log fájlba is írjuk
                    if LOG_WRITER:
                        try:
                            LOG_WRITER.write(f"\n{'='*80}\n")
                            LOG_WRITER.write(f"⚠⚠⚠ VÉGZETES HIBA ⚠⚠⚠\n")
                            LOG_WRITER.write(f"{'='*80}\n")
                            LOG_WRITER.write(f"{error_msg}\n")
                            LOG_WRITER.write(f"{'='*80}\n\n")
                            LOG_WRITER.flush()
                        except Exception:
                            pass
                    # Azonnali leállítás
                    STOP_EVENT.set()
                    self.graceful_stop_requested = True
                    # MessageBox hibaüzenet (GUI thread-ben)
                    self.root.after(0, lambda: messagebox.showerror(
                        "VÉGZETES HIBA",
                        error_msg
                    ))
                    # Várunk egy kicsit, hogy a MessageBox megjelenjen
                    time.sleep(0.5)
                    raise ValueError(error_msg)
                nvenc_fallback_requested = False
                try:
                    with console_redirect(nvenc_logger):
                        print(f"🔍 Kódolás fájl ellenőrzés (teljes útvonal): {video_path_abs_check}")
                        print(f"🎬 NVENC kódolás kezdése: {video_path.name}")
                        print(f"   Teljes útvonal: {video_path_abs_check}")
                        print(f"   Cél fájl: {output_file.absolute()}")
                        success_nvenc = encode_video(video_path, output_file, cq_value_nvenc, subtitle_files, 'av1_nvenc', progress_callback, initial_min_vmaf, vmaf_step, max_encoded, stop_event=STOP_EVENT, vmaf_value=vmaf_value_nvenc, resize_enabled=resize_enabled, resize_height=resize_height, audio_compression_enabled=audio_compression_enabled, audio_compression_method=audio_compression_method, logger=nvenc_logger)
                except NVENCFallbackRequired:
                    nvenc_fallback_requested = True
                    success_nvenc = False
                except EncodingStopped:
                    current_values = self.tree.item(item_id, 'values')
                    status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                    tags = self.tree.item(item_id, 'tags')
                    if "✓ Kész" not in status and "completed" not in tags and "Ellenőrizendő" not in status and "needs_check" not in tags:
                        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                        self.encoding_queue.put(("update", item_id, t('status_nvenc_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                        self.encoding_queue.put(("tag", item_id, "pending"))
                        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                    finish_nvenc_task()
                    continue
                except Exception as e:
                    # Általános hiba esetén SVT-AV1 fallback
                    with console_redirect(nvenc_logger):
                        print(f"\n⚠ NVENC kódolás hiba: {e}")
                        print(f"⚠ NVENC hiba → SVT-AV1 queue-ba helyezés")
                    success_nvenc = False
                    nvenc_fallback_requested = True
                
                if nvenc_fallback_requested:
                    with console_redirect(self.svt_logger):
                        print(f"\n⚠ NVENC fallback → SVT-AV1 queue-ba helyezés")
                    
                    if output_file.exists() and not DEBUG_MODE:
                        output_file.unlink()
                    
                    svt_task = {
                        'video_path': video_path,
                        'output_file': output_file,
                        'subtitle_files': subtitle_files,
                        'item_id': item_id,
                        'orig_size_str': orig_size_str,
                        'initial_min_vmaf': initial_min_vmaf,
                        'vmaf_step': vmaf_step,
                        'max_encoded': max_encoded,
                        'resize_enabled': resize_enabled,
                        'resize_height': resize_height,
                        'audio_compression_enabled': audio_compression_enabled,
                        'audio_compression_method': audio_compression_method,
                        'reason': 'nvenc_fallback_during_encode'
                    }
                    SVT_QUEUE.put(svt_task)
                    current_values = self.tree.item(item_id, 'values')
                    completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                    self.encoding_queue.put(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                    self.encoding_queue.put(("tag", item_id, "encoding_svt"))
                    # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                    finish_nvenc_task()
                    continue
                
                if not self.is_encoding:
                    current_values = self.tree.item(item_id, 'values')
                    orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
                    completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                    self.encoding_queue.put(("update", item_id, t('status_nvenc_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                    self.encoding_queue.put(("tag", item_id, "pending"))
                    # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                    finish_nvenc_task()
                    continue
                
                is_valid = False
                used_encoder = "NVENC"
                final_cq = cq_value_nvenc
                final_vmaf = vmaf_value_nvenc
                
                if not success_nvenc and not nvenc_fallback_requested:
                    # Ha a kódolás sikertelen volt, de nem volt fallback kérés, SVT-AV1 fallback
                    with console_redirect(self.svt_logger):
                        print(f"\n⚠ NVENC kódolás sikertelen → SVT-AV1 queue-ba helyezés")
                    
                    if output_file.exists() and not DEBUG_MODE:
                        output_file.unlink()
                    
                    svt_task = {
                        'video_path': video_path,
                        'output_file': output_file,
                        'subtitle_files': subtitle_files,
                        'item_id': item_id,
                        'orig_size_str': orig_size_str,
                        'initial_min_vmaf': initial_min_vmaf,
                        'vmaf_step': vmaf_step,
                        'max_encoded': max_encoded,
                        'resize_enabled': resize_enabled,
                        'resize_height': resize_height,
                        'audio_compression_enabled': audio_compression_enabled,
                        'audio_compression_method': audio_compression_method,
                        'reason': 'nvenc_encoding_failed'
                    }
                    SVT_QUEUE.put(svt_task)
                    current_values = self.tree.item(item_id, 'values')
                    completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                    self.encoding_queue.put(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                    self.encoding_queue.put(("tag", item_id, "encoding_svt"))
                    # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                    finish_nvenc_task()
                    continue
                
                if success_nvenc:
                    # Ellenőrizzük, hogy a videó már "Kész" állapotban van-e
                    current_values = self.tree.item(item_id, 'values')
                    status_before_validation = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                    tags_before_validation = self.tree.item(item_id, 'tags')
                    is_already_completed = (
                        "✓ Kész" in status_before_validation or 
                        "completed" in tags_before_validation or 
                        "Kész" in status_before_validation
                    )
                    
                    if is_already_completed:
                        is_valid = True
                        finish_nvenc_task()
                        continue
                    
                    completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                    vmaf_display = format_localized_number(vmaf_value_nvenc, decimals=1) if vmaf_value_nvenc is not None else "-"
                    self.encoding_queue.put(("update", item_id, "NVENC validálás...", str(int(cq_value_nvenc)), vmaf_display, "-", "100%", orig_size_str, "-", "-", completed_date))
                    
                    if not self.is_encoding:
                        current_values = self.tree.item(item_id, 'values')
                        orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
                        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                        self.encoding_queue.put(("update", item_id, t('status_nvenc_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                        self.encoding_queue.put(("tag", item_id, "pending"))
                        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                        finish_nvenc_task()
                        continue
                    
                    # Validálás
                    try:
                        with console_redirect(nvenc_logger):
                            is_valid = validate_encoded_video_vlc(output_file, encoder='av1_nvenc', stop_event=STOP_EVENT, source_path=video_path)
                    except EncodingStopped:
                        current_values = self.tree.item(item_id, 'values')
                        orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
                        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                        self.encoding_queue.put(("update", item_id, t('status_nvenc_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                        self.encoding_queue.put(("tag", item_id, "pending"))
                        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                        finish_nvenc_task()
                        continue
                    
                    # Újraellenőrizzük a validálás után is
                    current_values = self.tree.item(item_id, 'values')
                    status_after_validation = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else ""
                    tags_after_validation = self.tree.item(item_id, 'tags')
                    is_now_completed = (
                        "✓ Kész" in status_after_validation or 
                        "completed" in tags_after_validation or 
                        "Kész" in status_after_validation
                    )
                    
                    if is_now_completed:
                        is_valid = True
                        finish_nvenc_task()
                        continue
                    
                    if not self.is_encoding:
                        current_values = self.tree.item(item_id, 'values')
                        orig_size_str = current_values[self.COLUMN_INDEX['orig_size']] if len(current_values) > self.COLUMN_INDEX['orig_size'] else "-"
                        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                        self.encoding_queue.put(("update", item_id, t('status_nvenc_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                        self.encoding_queue.put(("tag", item_id, "pending"))
                        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                        finish_nvenc_task()
                        continue
                    
                    # Validálás eredményének feldolgozása
                    if is_valid is None:
                        # SVT queue-ba kerülés
                        with console_redirect(self.svt_logger):
                            print(f"\n⚠ NVENC 'unexpected end of stream' → SVT-AV1 queue-ba")
                        
                        if output_file.exists() and not DEBUG_MODE:
                            output_file.unlink()
                        
                        svt_task = {
                            'video_path': video_path,
                            'output_file': output_file,
                            'subtitle_files': subtitle_files,
                            'item_id': item_id,
                            'orig_size_str': orig_size_str,
                            'initial_min_vmaf': initial_min_vmaf,
                            'vmaf_step': vmaf_step,
                            'max_encoded': max_encoded,
                            'resize_enabled': resize_enabled,
                            'resize_height': resize_height,
                            'audio_compression_enabled': audio_compression_enabled,
                            'audio_compression_method': audio_compression_method,
                            'reason': 'unexpected_end'
                        }
                        SVT_QUEUE.put(svt_task)
                        current_values = self.tree.item(item_id, 'values')
                        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                        self.encoding_queue.put(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                        self.encoding_queue.put(("tag", item_id, "encoding_svt"))
                        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                        finish_nvenc_task()
                        continue
                    elif not is_valid:
                        # SVT queue-ba kerülés
                        with console_redirect(self.svt_logger):
                            print(f"\n⚠ NVENC érvénytelen → SVT-AV1 queue-ba")
                        
                        if output_file.exists() and not DEBUG_MODE:
                            output_file.unlink()
                        
                        svt_task = {
                            'video_path': video_path,
                            'output_file': output_file,
                            'subtitle_files': subtitle_files,
                            'item_id': item_id,
                            'orig_size_str': orig_size_str,
                            'initial_min_vmaf': initial_min_vmaf,
                            'vmaf_step': vmaf_step,
                            'max_encoded': max_encoded,
                            'resize_enabled': resize_enabled,
                            'resize_height': resize_height,
                            'audio_compression_enabled': audio_compression_enabled,
                            'audio_compression_method': audio_compression_method,
                            'reason': 'invalid'
                        }
                        SVT_QUEUE.put(svt_task)
                        current_values = self.tree.item(item_id, 'values')
                        completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                        self.encoding_queue.put(("update", item_id, t('status_svt_queue'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                        self.encoding_queue.put(("tag", item_id, "encoding_svt"))
                        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                        finish_nvenc_task()
                        continue
                
                if is_valid:
                    orig_size_mb, new_size_mb, change_percent = self.calculate_file_sizes(video_path, output_file)
                    orig_size_display = f"{format_localized_number(orig_size_mb, decimals=1)} MB"
                    status_text = get_completed_status_for_encoder(used_encoder)
                    final_vmaf_display = format_localized_number(final_vmaf, decimals=1)
                    self.mark_encoding_completed(item_id, status_text, str(int(final_cq)), final_vmaf_display, "-", orig_size_display, new_size_mb, change_percent)
                    self._copy_invalid_subtitles(invalid_subtitles, output_file)
                    
                    with self.nvenc_worker_stats_lock:
                        self.nvenc_worker_stats['completed'] += 1
                else:
                    current_values = self.tree.item(item_id, 'values')
                    completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                    self.clear_encoding_times(item_id)
                    self.encoding_queue.put(("update", item_id, t('status_failed'), "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                    self.encoding_queue.put(("tag", item_id, "failed"))
                    self.encoding_queue.put(("progress_bar", 0))
                    # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                    
                    with self.nvenc_worker_stats_lock:
                        self.nvenc_worker_stats['failed'] += 1
                    
                    if output_file.exists() and not DEBUG_MODE:
                        output_file.unlink()
                
                with console_redirect(nvenc_logger):
                    print(f"✓ NVENC worker #{worker_index + 1} slot felszabadítva\n")
                
                finish_nvenc_task()
        finally:
            # Aktív videó eltávolítása és processing set tisztítása (ha még ott van)
            with self.nvenc_selection_lock:
                self.nvenc_active_videos.discard(worker_index)
                # Ha a worker váratlanul kilépett, eltávolítjuk az aktuális videót a processing set-ből
                if current_video_path:
                    self.nvenc_processing_videos.discard(current_video_path)
        
        if self.graceful_stop_requested and not STOP_EVENT.is_set():
            with console_redirect(nvenc_logger):
                print(f"\n🟡 Leállítás kérése → NVENC worker #{worker_index + 1} megszakítva\n")
        
        with console_redirect(nvenc_logger):
            print(f"\n{'#'*80}\n### NVENC WORKER #{worker_index + 1} BEFEJEZVE ###\n{'#'*80}\n")

    def vmaf_worker(self):
        """Background worker for VMAF/PSNR calculation tasks.

        Processes requests for VMAF/PSNR calculation on encoded videos.
        """

        set_low_priority()
        
        debug_pause.gui_queue = self.encoding_queue
        self.vmaf_worker_active = True

        def schedule_vmaf_idle():
            if hasattr(self, 'root'):
                self.root.after(0, self._on_vmaf_worker_finished)
        
        while True:
            # Azonnali leállítás ellenőrzése
            if STOP_EVENT.is_set():
                # Azonnali leállítás: minden VMAF számítás megszakítása
                with console_redirect(self.svt_logger):
                    print(f"\n🛑 Azonnali leállítás → VMAF/PSNR worker megszakítva\n")
                # Visszaállítjuk a queue-ban lévő tételek státuszát és visszasorozzuk őket
                pending_tasks = []
                while not VMAF_QUEUE.empty():
                    try:
                        task = VMAF_QUEUE.get_nowait()
                    except queue.Empty:
                        break
                    pending_tasks.append(task)
                    item_id = task.get('item_id')
                    if item_id:
                        current_values = self.get_tree_values(item_id, min_length=10)
                        waiting_status = self._get_vmaf_waiting_status_text(bool(task.get('check_vmaf', True)), bool(task.get('check_psnr', True)))
                        self.encoding_queue.put((
                            "update",
                            item_id,
                            waiting_status,
                            current_values[self.COLUMN_INDEX['cq']],
                            current_values[self.COLUMN_INDEX['vmaf']],
                            current_values[self.COLUMN_INDEX['psnr']],
                            "-",
                            current_values[self.COLUMN_INDEX['orig_size']],
                            current_values[self.COLUMN_INDEX['new_size']],
                            current_values[self.COLUMN_INDEX['size_change']],
                            current_values[self.COLUMN_INDEX['completed_date']]
                        ))
                        self.encoding_queue.put(("tag", item_id, "pending"))
                    VMAF_QUEUE.task_done()
                for queued_task in pending_tasks:
                    VMAF_QUEUE.put(queued_task)
                if pending_tasks:
                    # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                    pass
                # Leállítás gombok inaktiválása, ha nincs más aktív folyamat
                schedule_vmaf_idle()
                break
            
            # Sima leállítás ellenőrzése
            if self.graceful_stop_requested:
                if VMAF_QUEUE.empty():
                    # Ha nincs több feladat, befejezhetjük
                    with console_redirect(self.svt_logger):
                        print(f"\n🟡 Leállítás kérése → VMAF/PSNR worker befejezve (nincs több feladat)\n")
                    schedule_vmaf_idle()
                    break
                # Ha van még feladat, folytatjuk az aktuális feladat feldolgozását, de nem veszünk fel újat
            
            try:
                task = VMAF_QUEUE.get(timeout=2)
            except queue.Empty:
                if STOP_EVENT.is_set() or (self.graceful_stop_requested and VMAF_QUEUE.empty()) or not self.is_encoding:
                    schedule_vmaf_idle()
                    break
                continue
            
            video_path = task['video_path']
            output_file = task['output_file']
            item_id = task['item_id']
            orig_size_str = task['orig_size_str']
            check_vmaf = bool(task.get('check_vmaf', True))
            check_psnr = bool(task.get('check_psnr', True))
            if not check_vmaf and not check_psnr:
                check_vmaf = True

            pending_vmaf = bool(check_vmaf)
            pending_psnr = bool(check_psnr)

            # Újraellenőrizzük az azonnali leállítást
            if STOP_EVENT.is_set():
                waiting_status = self._get_vmaf_waiting_status_text(check_vmaf, check_psnr)
                current_values = self.get_tree_values(item_id, min_length=10)
                self.encoding_queue.put((
                    "update",
                    item_id,
                    waiting_status,
                    current_values[self.COLUMN_INDEX['cq']],
                    current_values[self.COLUMN_INDEX['vmaf']],
                    current_values[self.COLUMN_INDEX['psnr']],
                    "-",
                    current_values[self.COLUMN_INDEX['orig_size']],
                    current_values[self.COLUMN_INDEX['new_size']],
                    current_values[self.COLUMN_INDEX['size_change']],
                    current_values[self.COLUMN_INDEX['completed_date']]
                ))
                self.encoding_queue.put(("tag", item_id, "pending"))
                # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                VMAF_QUEUE.task_done()
                VMAF_QUEUE.put(task)
                schedule_vmaf_idle()
                break
            
            # Ellenőrizzük, hogy a fájlok léteznek-e
            if not video_path.exists() or not output_file.exists():
                current_values = self.tree.item(item_id, 'values')
                completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                self.encoding_queue.put(("update", item_id, "✗ Fájl hiányzik", "-", "-", "-", "-", orig_size_str, "-", "-", completed_date))
                self.encoding_queue.put(("tag", item_id, "failed"))
                # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                VMAF_QUEUE.task_done()
                continue
            
            # KRITIKUS: CPU worker lock - biztosítja, hogy csak 1 CPU worker (SVT-AV1 vagy VMAF/PSNR) fusson egyszerre
            with CPU_WORKER_LOCK:
                # KRITIKUS: A lock-on belül dolgozunk, hogy egyesével történjen a feldolgozás
                # és csak az aktív videó legyen "folyamatban" státuszban
                # Slot megszerzése - egyesével dolgozunk
                current_values = self.tree.item(item_id, 'values')
                # Eredeti értékek elmentése (VMAF számítás előtti állapot)
                original_status = current_values[self.COLUMN_INDEX['status']] if len(current_values) > self.COLUMN_INDEX['status'] else "✓ Kész"
                original_completed_date = current_values[self.COLUMN_INDEX['completed_date']] if len(current_values) > self.COLUMN_INDEX['completed_date'] else ""
                original_cq_str = current_values[self.COLUMN_INDEX['cq']] if len(current_values) > self.COLUMN_INDEX['cq'] else "-"
                original_vmaf_str = current_values[self.COLUMN_INDEX['vmaf']] if len(current_values) > self.COLUMN_INDEX['vmaf'] else "-"
                original_psnr_str = current_values[self.COLUMN_INDEX['psnr']] if len(current_values) > self.COLUMN_INDEX['psnr'] else "-"
                original_new_size_str = current_values[self.COLUMN_INDEX['new_size']] if len(current_values) > self.COLUMN_INDEX['new_size'] else "-"
                original_size_change = current_values[self.COLUMN_INDEX['size_change']] if len(current_values) > self.COLUMN_INDEX['size_change'] else "-"
                
                # KRITIKUS: Ha a méret vagy változás "-", számoljuk ki az output fájlból!
                if (original_new_size_str == "-" or original_size_change == "-") and output_file.exists():
                    try:
                        orig_size_mb, new_size_mb, change_percent = self.calculate_file_sizes(video_path, output_file)
                        if original_new_size_str == "-" and new_size_mb > 0:
                            original_new_size_str = f"{format_localized_number(new_size_mb, decimals=1)} MB"
                        if original_size_change == "-" and orig_size_mb > 0:
                            original_size_change = f"{format_localized_number(change_percent, decimals=2, show_sign=True)}%"
                    except Exception:
                        pass  # Ha nem sikerül kiszámolni, az eredeti értékeket használjuk
                
                # Ellenőrizzük, hogy az azonnali leállítás nincs-e már beállítva
                if STOP_EVENT.is_set():
                    # Azonnali leállítás már be van állítva, ne kezdjük el a VMAF számítást
                    # Eredeti értékek visszaállítása
                    self.encoding_queue.put(("update", item_id, original_status, original_cq_str, original_vmaf_str, original_psnr_str, "-", orig_size_str, original_new_size_str, original_size_change, original_completed_date))
                    self.encoding_queue.put(("tag", item_id, "pending"))
                    # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                    VMAF_QUEUE.task_done()
                    continue
                
                # Státusz beállítása "VMAF számítás folyamatban..."-ra
                # KRITIKUS: Csak az aktív videó legyen "folyamatban", a többi maradjon "vár"-ban
                # A "Befejezés" oszlopban "-" jelenik meg az első 10 másodpercben, amíg a timer nem számolja ki a becsült befejezési időt
                # KRITIKUS: AZONNAL, SZINKRON MÓDON állítsuk be a tree-ben, ne queue-n keresztül!
                # (a calculate_full_vmaf azonnal elindul, és a progress_callback látja a tree aktuális állapotát)
                current_values_list = list(current_values)
                if len(current_values_list) < 11:
                    current_values_list.extend([''] * (11 - len(current_values_list)))
                current_values_list[1] = t('status_vmaf_calculating')  # Státusz
                current_values_list[5] = "-"  # Progress
                current_values_list[9] = "-"  # Completed_date - KRITIKUS!
                self.tree.item(item_id, values=tuple(current_values_list))
                
                # Queue-ba is tegyük (konzisztencia miatt, de ez később dolgozódik fel)
                self.encoding_queue.put(("update", item_id, t('status_vmaf_calculating'), original_cq_str, original_vmaf_str, original_psnr_str, "-", orig_size_str, original_new_size_str, original_size_change, "-"))
                
                # Kezdési időpont tárolása
                self.encoding_start_times[item_id] = time.time()
                
                # KRITIKUS: Mentjük el az értékeket a progress_callback számára
                # NE olvassuk be a tree-t a worker threadben (nem thread-safe)!
                callback_cq_str = original_cq_str
                callback_vmaf_str = original_vmaf_str
                callback_psnr_str = original_psnr_str
                callback_orig_size_str = orig_size_str
                callback_new_size_str = original_new_size_str
                callback_size_change = original_size_change
                video_duration_seconds, _ = get_video_info(video_path)
                if video_duration_seconds is not None and video_duration_seconds <= 0:
                    video_duration_seconds = None
                total_duration_str = format_seconds_hms(video_duration_seconds) if video_duration_seconds else None
                metric_results = {'vmaf': None, 'psnr': None}
                metadata_updated_once = False
                current_progress_message = "-"
                current_status_display = t('status_vmaf_calculating')

                def update_metadata_partial():
                    nonlocal metadata_updated_once
                    if metric_results['vmaf'] is None:
                        return
                    with console_redirect(self.svt_logger):
                        update_video_metadata_vmaf(output_file, metric_results['vmaf'], psnr_value=metric_results['psnr'], logger=self.svt_logger)
                    metadata_updated_once = True

                def push_partial_update():
                    completed_date_to_use = self.estimated_end_dates.get(item_id, "-")
                    self.encoding_queue.put(("update", item_id, current_status_display, callback_cq_str, callback_vmaf_str, callback_psnr_str, current_progress_message, callback_orig_size_str, callback_new_size_str, callback_size_change, completed_date_to_use))
                    # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik

                def metric_done_callback(metric_name, value):
                    nonlocal callback_vmaf_str, callback_psnr_str, current_status_display, pending_vmaf, pending_psnr
                    metric_upper = (metric_name or "").upper()
                    if metric_upper == 'VMAF':
                        metric_results['vmaf'] = value
                        callback_vmaf_str = format_metric_value(value)
                        pending_vmaf = False
                        update_metadata_partial()
                        if check_psnr:
                            current_status_display = t('status_psnr_only')
                        push_partial_update()
                    elif metric_upper in ('PSNR', 'XPSNR'):
                        metric_results['psnr'] = value
                        callback_psnr_str = format_metric_value(value)
                        pending_psnr = False
                        if metric_results['vmaf'] is None:
                            try:
                                _, output_vmaf_meta, _, _, _, _, _, _, _ = get_output_file_info(output_file)
                                if output_vmaf_meta is not None:
                                    metric_results['vmaf'] = output_vmaf_meta
                                    callback_vmaf_str = format_metric_value(output_vmaf_meta)
                            except Exception:
                                pass
                        update_metadata_partial()
                        push_partial_update()

                def progress_callback(msg):
                    nonlocal current_status_display, current_progress_message
                    # Ellenőrizzük, hogy az azonnali leállítás nincs-e beállítva
                    if STOP_EVENT.is_set():
                        return
                    # KRITIKUS: NE olvassuk a tree-t (nem thread-safe worker threadben)!
                    # Használjuk az elmentett értékeket
                    status = t('status_vmaf_calculating')
                    progress_display = "-"
                    completed_date_to_use = self.estimated_end_dates.get(item_id, "-")

                    if isinstance(msg, dict) and msg.get('type') == 'abav1_progress':
                        metric_name = msg.get('metric')
                        if metric_name == 'VMAF':
                            status = t('status_vmaf_only')
                        elif metric_name in ('XPSNR', 'PSNR'):
                            status = t('status_psnr_only')
                        percent = msg.get('percent')
                        eta_seconds = msg.get('eta_seconds')
                        duration_for_calc = video_duration_seconds if video_duration_seconds else msg.get('duration_seconds')
                        elapsed_seconds = msg.get('elapsed_seconds')
                        if elapsed_seconds is None and percent is not None and duration_for_calc:
                            elapsed_seconds = max(0.0, min(duration_for_calc, duration_for_calc * (percent / 100.0)))
                        elapsed_str = format_seconds_hms(elapsed_seconds) if elapsed_seconds is not None else None
                        total_str = total_duration_str or format_seconds_hms(duration_for_calc)
                        if elapsed_str and total_str:
                            progress_display = f"{elapsed_str} / {total_str}"
                        elif percent is not None:
                            progress_display = f"{format_localized_number(percent, decimals=1)}%"
                        else:
                            progress_display = msg.get('text', "-")

                        if eta_seconds is not None:
                            try:
                                eta_seconds = float(max(0.0, eta_seconds))
                                estimated_end_datetime = datetime.fromtimestamp(time.time() + eta_seconds)
                                estimated_end_str = estimated_end_datetime.strftime("%Y-%m-%d %H:%M:%S")
                                self.estimated_end_dates[item_id] = estimated_end_str
                                completed_date_to_use = estimated_end_str
                            except (ValueError, OverflowError):
                                completed_date_to_use = self.estimated_end_dates.get(item_id, "-")
                        else:
                            completed_date_to_use = self.estimated_end_dates.get(item_id, "-")
                    else:
                        progress_display = msg if isinstance(msg, str) else str(msg)
                        completed_date_to_use = self.estimated_end_dates.get(item_id, "-")

                    current_status_display = status
                    current_progress_message = progress_display
                    self.encoding_queue.put(("update", item_id, status, callback_cq_str, callback_vmaf_str, callback_psnr_str, progress_display, callback_orig_size_str, callback_new_size_str, callback_size_change, completed_date_to_use))
                
                # KRITIKUS: A calculate_full_vmaf() hívás a CPU_WORKER_LOCK-on BELÜL történik,
                # hogy ne fusson egyszerre SVT-AV1 és VMAF számítás
                # De a VMAF_LOCK-ot kiengedjük, hogy ne blokkoljuk a státusz frissítéseket
                try:
                    # VMAF számítás SVT-AV1 konzolba (CPU-s worker)
                    with console_redirect(self.svt_logger):
                        print(f"\n{'='*80}")
                        print(f"📊 VMAF/PSNR TESZT: {video_path.name}")
                        print(f"{'='*80}")
                        vmaf_result = calculate_full_vmaf(
                            video_path,
                            output_file,
                            progress_callback,
                            STOP_EVENT,
                            logger=self.svt_logger,
                            check_vmaf=check_vmaf,
                            check_psnr=check_psnr,
                            metric_done_callback=metric_done_callback
                        )
                        if vmaf_result:
                            vmaf_value, psnr_value = vmaf_result
                        else:
                            vmaf_value, psnr_value = (None, None)
                    
                    final_vmaf_value = metric_results['vmaf'] if metric_results['vmaf'] is not None else vmaf_value
                    final_psnr_value = metric_results['psnr'] if metric_results['psnr'] is not None else psnr_value
                    metrics_ok = True
                    if check_vmaf and final_vmaf_value is None:
                        metrics_ok = False
                    if check_psnr and final_psnr_value is None:
                        metrics_ok = False
                    
                    if metrics_ok:
                        if not metadata_updated_once and final_vmaf_value is not None:
                            with console_redirect(self.svt_logger):
                                update_video_metadata_vmaf(output_file, final_vmaf_value, psnr_value=final_psnr_value, logger=self.svt_logger)
                                metadata_updated_once = True
                        # Célfájl információinak kiolvasása (CQ/CRF, fájlméret, változás)
                        output_cq_crf, output_vmaf_meta, output_psnr_meta, output_frame_count, output_file_size, output_modified_date, output_encoder_type, _, _ = get_output_file_info(output_file)
                        
                        # CQ/CRF érték - ha van a célfájlban, azt használjuk, különben az eredeti
                        final_cq_str = original_cq_str
                        if output_cq_crf is not None:
                            final_cq_str = str(output_cq_crf)
                        elif final_cq_str == "-":
                            # Ha az eredeti is "-", próbáljuk a metaadatból kiolvasni
                            try:
                                probe_cmd = [FFPROBE_PATH, '-v', 'error', '-show_entries', 'format_tags=Settings', '-of', 'default=noprint_wrappers=1:nokey=1', os.fspath(output_file.absolute())]
                                result = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=5)
                                settings_metadata = result.stdout.strip()
                                cq_match = re.search(r'CQ:(\d+)', settings_metadata)
                                crf_match = re.search(r'CRF:(\d+)', settings_metadata)
                                if cq_match:
                                    final_cq_str = cq_match.group(1)
                                elif crf_match:
                                    final_cq_str = crf_match.group(1)
                            except (AttributeError, ValueError, TypeError):
                                pass
                        
                        # Fájlméret és változás - ha van a célfájlban, azt használjuk
                        final_new_size_str = original_new_size_str
                        final_size_change = original_size_change
                        if output_file_size is not None:
                            new_size_mb = output_file_size / (1024**2)
                            final_new_size_str = f"{format_localized_number(new_size_mb, decimals=1)} MB"
                            
                            # Változás számítása, ha van eredeti méret
                            if orig_size_str and orig_size_str != "-" and 'MB' in orig_size_str:
                                try:
                                    orig_size_val = float(orig_size_str.replace(' MB', ''))
                                    change_percent = ((new_size_mb - orig_size_val) / orig_size_val) * 100 if orig_size_val > 0 else 0
                                    final_size_change = f"{format_localized_number(change_percent, decimals=2, show_sign=True)}%"
                                except (ValueError, TypeError, ZeroDivisionError, AttributeError):
                                    pass
                        
                        # Táblázat frissítés - eredeti értékek visszaállítása + frissített VMAF
                        # Ha várakozó státusz volt, akkor a kódoló típusa alapján határozzuk meg a helyes "Kész" státuszt
                        final_status_code_hint = task.get('final_status_code')
                        status_hint = None
                        if final_status_code_hint:
                            localized_hint = status_code_to_localized(final_status_code_hint)
                            if localized_hint and normalize_status_to_code(localized_hint) == final_status_code_hint:
                                status_hint = localized_hint
                        
                        status_code = normalize_status_to_code(original_status)
                        if status_hint:
                            status = status_hint
                        elif status_code in ('vmaf_waiting', 'vmaf_psnr_waiting', 'psnr_waiting'):
                            # A fájl metadata-jából meghatározzuk a kódoló típusát
                            if output_encoder_type:
                                if output_encoder_type == 'nvenc':
                                    status = t('status_completed_nvenc')
                                elif output_encoder_type == 'svt-av1':
                                    status = t('status_completed_svt')
                                else:
                                    status = t('status_completed')
                            else:
                                try:
                                    probe_cmd = [FFPROBE_PATH, '-v', 'error', '-show_entries', 'format_tags=Settings', '-of', 'default=noprint_wrappers=1:nokey=1', os.fspath(output_file.absolute())]
                                    result = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=5)
                                    settings_metadata = result.stdout.strip()
                                    if 'SVT-AV1' in settings_metadata or 'svt-av1' in settings_metadata.lower():
                                        status = t('status_completed_svt')
                                    elif 'NVENC' in settings_metadata:
                                        status = t('status_completed_nvenc')
                                    else:
                                        status = t('status_completed')
                                except (subprocess.SubprocessError, OSError, ValueError, AttributeError, FileNotFoundError):
                                    # Ha nem sikerül a metadata olvasása, alapértelmezett "Kész" státusz
                                    status = t('status_completed')
                        else:
                            # Ha az original_status nem "VMAF ellenőrzésre vár...", akkor azt használjuk
                            status = original_status
                        # Eredeti completed_date visszaállítása (az átkódolás befejezési dátuma, nem a becsült VMAF befejezési idő)
                        # Ha van output_modified_date, azt használjuk
                        final_completed_date = output_modified_date if output_modified_date else original_completed_date
                        # VMAF és PSNR külön kezelése
                        if final_psnr_value is not None:
                            vmaf_display = format_metric_value(final_vmaf_value) if final_vmaf_value is not None else (format_metric_value(output_vmaf_meta) if output_vmaf_meta is not None else callback_vmaf_str)
                            psnr_display = format_metric_value(final_psnr_value)
                        elif output_psnr_meta is not None:
                            if output_vmaf_meta is not None:
                                vmaf_display = format_metric_value(output_vmaf_meta)
                            elif final_vmaf_value is not None:
                                vmaf_display = format_metric_value(final_vmaf_value)
                            else:
                                vmaf_display = callback_vmaf_str
                            psnr_display = format_metric_value(output_psnr_meta)
                        else:
                            vmaf_display = format_metric_value(final_vmaf_value) if final_vmaf_value is not None else (format_metric_value(output_vmaf_meta) if output_vmaf_meta is not None else callback_vmaf_str)
                            psnr_display = callback_psnr_str if callback_psnr_str not in ("", None, "-") else "-"
                        self.encoding_queue.put(("update", item_id, status, final_cq_str, vmaf_display, psnr_display, "100%", orig_size_str, final_new_size_str, final_size_change, final_completed_date))
                        
                        self.encoding_queue.put(("tag", item_id, "completed"))
                        
                        # Adatbázis frissítése VMAF/PSNR mérés befejezése után
                        # Számoljuk ki a new_size_mb-t és change_percent-et az adatbázis frissítéshez
                        # Használjuk az output_file_size-t (byte-ban), ne parse-oljunk MB stringet!
                        new_size_mb = None
                        change_percent = None
                        if output_file_size is not None:
                            new_size_mb = output_file_size / (1024**2)
                        elif final_new_size_str and final_new_size_str != "-" and 'MB' in final_new_size_str:
                            # Fallback: csak akkor parse-olunk, ha nincs output_file_size
                            try:
                                new_size_mb = float(final_new_size_str.replace(' MB', '').replace(',', '.'))
                            except (ValueError, TypeError):
                                pass
                        if final_size_change and final_size_change != "-" and '%' in final_size_change:
                            try:
                                change_percent = float(final_size_change.replace('%', '').replace('+', '').replace(',', '.'))
                            except (ValueError, TypeError):
                                pass
                        
                        if video_path:
                            # Háttérszálban frissítjük az adatbázist
                            def update_db_after_vmaf():
                                try:
                                    self.update_single_video_in_db(
                                        video_path, item_id, status, final_cq_str, 
                                        vmaf_display, psnr_display, orig_size_str, 
                                        new_size_mb, change_percent, final_completed_date
                                    )
                                except Exception as e:
                                    # Csendes hiba - ne zavarjuk meg a VMAF folyamatot
                                    if LOG_WRITER:
                                        try:
                                            LOG_WRITER.write(f"⚠ [vmaf_worker] Adatbázis frissítés hiba: {e} | video: {video_path}\n")
                                            LOG_WRITER.flush()
                                        except Exception:
                                            pass
                            
                            db_thread = threading.Thread(target=update_db_after_vmaf, daemon=True)
                            db_thread.start()
                        
                        # Kezdési időpont és becsült befejezési idő törlése
                        self.clear_encoding_times(item_id)
                        
                        # VMAF/PSNR worker STOP: Ha nincs több VMAF task, állítsuk le az is_encoding flaget
                        if VMAF_QUEUE.empty():
                            schedule_vmaf_idle()
                        
                        with console_redirect(self.svt_logger):
                            if check_psnr and final_psnr_value is not None:
                                print(f"\n✓ VMAF/PSNR teszt kész: {video_path.name} - VMAF: {format_metric_value(final_vmaf_value) if final_vmaf_value is not None else '-'} / PSNR: {format_metric_value(final_psnr_value)}\n")
                            elif final_vmaf_value is not None and check_vmaf:
                                print(f"\n✓ VMAF teszt kész: {video_path.name} - VMAF: {format_metric_value(final_vmaf_value)}\n")
                            elif check_psnr and final_psnr_value is not None:
                                print(f"\n✓ PSNR teszt kész: {video_path.name} - PSNR: {format_metric_value(final_psnr_value)}\n")
                    else:
                        # VMAF/PSNR számítás hiba - eredeti értékek visszaállítása
                        self.encoding_queue.put(("update", item_id, original_status, original_cq_str, original_vmaf_str, original_psnr_str, "-", orig_size_str, original_new_size_str, original_size_change, original_completed_date))
                        self.encoding_queue.put(("tag", item_id, "failed"))
                        # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                        
                        # Kezdési időpont és becsült befejezési idő törlése
                        self.clear_encoding_times(item_id)
                        
                        # VMAF/PSNR worker STOP: Ha nincs több VMAF task, állítsuk le az is_encoding flaget
                        if VMAF_QUEUE.empty():
                            schedule_vmaf_idle()
                        
                        with console_redirect(self.svt_logger):
                            print(f"\n? VMAF/PSNR számítás hiba: {video_path.name}\n")
                
                except EncodingStopped:
                    pending_vmaf_flag = bool(pending_vmaf)
                    pending_psnr_flag = bool(pending_psnr)
                    if pending_vmaf_flag or pending_psnr_flag:
                        waiting_status = self._get_vmaf_waiting_status_text(pending_vmaf_flag, pending_psnr_flag)
                    else:
                        waiting_status = original_status
                    display_cq = callback_cq_str if callback_cq_str not in ("", None) else original_cq_str
                    display_vmaf = callback_vmaf_str if callback_vmaf_str not in ("", None) else original_vmaf_str
                    display_psnr = callback_psnr_str if callback_psnr_str not in ("", None) else original_psnr_str
                    self.encoding_queue.put((
                        "update",
                        item_id,
                        waiting_status,
                        display_cq,
                        display_vmaf,
                        display_psnr,
                        "-",
                        orig_size_str,
                        callback_new_size_str,
                        callback_size_change,
                        original_completed_date
                    ))
                    self.encoding_queue.put(("tag", item_id, "pending"))
                    # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                    stop_msg = (
                        f"\n🛑 Azonnali leállítás → VMAF számítás megszakítva: {video_path.name}\n"
                        if STOP_EVENT.is_set()
                        else f"\n🟡 VMAF számítás megszakítva: {video_path.name}\n"
                    )
                    with console_redirect(self.svt_logger):
                        print(stop_msg)
                    if pending_vmaf_flag or pending_psnr_flag:
                        retry_task = dict(task)
                        retry_task['check_vmaf'] = pending_vmaf_flag
                        retry_task['check_psnr'] = pending_psnr_flag
                        VMAF_QUEUE.put(retry_task)
                    # Kezdési időpont és becsült befejezési idő törlése
                    self.clear_encoding_times(item_id)
                    
                    # VMAF/PSNR worker STOP: Ha nincs több VMAF task, állítsuk le az is_encoding flaget
                    if VMAF_QUEUE.empty():
                        schedule_vmaf_idle()
                    
                    VMAF_QUEUE.task_done()
                    # Ha azonnali leállítás van, kilépünk
                    if STOP_EVENT.is_set():
                        schedule_vmaf_idle()
                        break
                    continue
                except Exception as e:
                    # Hiba esetén eredeti értékek visszaállítása
                    self.encoding_queue.put(("update", item_id, original_status, original_cq_str, original_vmaf_str, original_psnr_str, "-", orig_size_str, original_new_size_str, original_size_change, original_completed_date))
                    self.encoding_queue.put(("tag", item_id, "failed"))
                    # save_json hivatkozások eltávolítva - adatbázis mentés csak start_encoding és stop_encoding-ban történik
                    
                    # Kezdési időpont és becsült befejezési idő törlése
                    self.clear_encoding_times(item_id)
                    
                    # VMAF/PSNR worker STOP: Ha nincs több VMAF task, állítsuk le az is_encoding flaget
                    if VMAF_QUEUE.empty():
                        schedule_vmaf_idle()
                    
                    with console_redirect(self.svt_logger):
                        print(f"\n✗ VMAF teszt hiba: {e}\n")
            
            VMAF_QUEUE.task_done()
            
            # Sima leállítás ellenőrzése a feladat után
            if self.graceful_stop_requested and VMAF_QUEUE.empty():
                with console_redirect(self.svt_logger):
                    print(f"\n🟡 Leállítás kérése → VMAF/PSNR worker befejezve\n")
                schedule_vmaf_idle()
                break
            
            # Ha nincs több VMAF feladat és nincs aktív kódolás, gombok inaktiválása
            if VMAF_QUEUE.empty() and not self.is_encoding:
                schedule_vmaf_idle()
                break

def main(short_test=False):
    print(f"\n{'='*80}\nAV1 BATCH ENCODER\n{'='*80}\n")
    root = tk.Tk()
    VideoEncoderGUI(root)

    if short_test:
        def _short_test_exit():
            print("\n=== SHORTTEST mód: 10 másodperc eltelt, automatikus kilépés ===\n")
            try:
                root.quit()
            except Exception:
                pass
            try:
                root.destroy()
            except Exception:
                pass
        root.after(10_000, _short_test_exit)

    root.mainloop()

if __name__ == "__main__":
    # Egyszerű kapcsolók a konzolos futtatáshoz és a gyors kilépéshez
    force_console = False
    short_test = os.environ.get("AV1_SHORT_TEST") == "1"
    load_debug_flag = LOAD_DEBUG
    cleaned_args = [sys.argv[0]]
    for arg in sys.argv[1:]:
        arg_lower = arg.lower()
        if arg_lower in ("-forceconsole", "--forceconsole", "--force-console"):
            force_console = True
            continue
        if arg_lower in ("-shorttest", "--shorttest", "--short-test"):
            short_test = True
            os.environ["AV1_SHORT_TEST"] = "1"
            continue
        if arg_lower in ("--load-debug", "--loaddebug", "-loaddebug"):
            load_debug_flag = True
            continue
        if arg_lower in ("--videoloading", "--video-loading", "-videoloading"):
            os.environ["AV1_VIDEO_LOADING_DEBUG"] = "1"
            globals()['VIDEO_LOADING_DEBUG'] = True
            continue
        cleaned_args.append(arg)
    if force_console:
        os.environ["AV1_FORCE_CONSOLE"] = "1"
    if load_debug_flag:
        os.environ["AV1_LOAD_DEBUG"] = "1"
        globals()['LOAD_DEBUG'] = True
    sys.argv = cleaned_args
    # Naplózás fájlba - ELŐBB mentjük az eredeti stdout/stderr-t
    original_stdout = sys.__stdout__  # Eredeti stdout (nem a ThreadSafeStdoutRouter)
    original_stderr = sys.__stderr__  # Eredeti stderr
    
    log_file = open("av1_recompress.log", "w", encoding="utf-8")
    
    # Globális LOG_WRITER beállítása
    globals()['LOG_WRITER'] = log_file
    
    # Video loading log inicializálása
    if VIDEO_LOADING_DEBUG:
        init_video_loading_log()
    
    class TeeOutput:
        def __init__(self, file_obj, original):
            self.file = file_obj
            self.original = original
        
        def write(self, data):
            try:
                self.file.write(data)
                self.file.flush()
            except (OSError, IOError, AttributeError):
                pass
            if self.original:
                try:
                    self.original.write(data)
                    self.original.flush()
                except (OSError, IOError, AttributeError):
                    pass
        
        def flush(self):
            try:
                self.file.flush()
            except (OSError, IOError, AttributeError):
                pass
            if self.original:
                try:
                    self.original.flush()
                except (OSError, IOError, AttributeError):
                    pass
    
    # STDOUT/STDERR átirányítás - használjuk az eredeti streameket
    tee_stdout = TeeOutput(log_file, original_stdout)
    tee_stderr = TeeOutput(log_file, original_stderr)
    
    # Írunk a log fájlba
    tee_stdout.write("=== PROGRAM INDÍTÁSA ===\n")
    tee_stdout.write(f"Python verzió: {sys.version}\n")
    tee_stdout.write(f"Futtatható: {sys.executable}\n")
    tee_stdout.write(f"Platform: {sys.platform}\n")

    if sys.platform == "win32" and not os.environ.get("AV1_FORCE_CONSOLE"):
        tee_stdout.write("pythonw.exe ellenőrzés...\n")
        if not sys.executable.endswith("pythonw.exe"):
            pythonw_exe = os.path.join(os.path.dirname(sys.executable), "pythonw.exe")
            tee_stdout.write(f"pythonw.exe útvonal: {pythonw_exe}\n")
            if os.path.exists(pythonw_exe):
                tee_stdout.write("pythonw.exe létezik, újraindítás...\n")
                log_file.close()
                subprocess.Popen([pythonw_exe, __file__])
                sys.exit(0)
            else:
                tee_stdout.write("pythonw.exe NEM létezik\n")
        else:
            tee_stdout.write("Már pythonw.exe-ben futunk\n")

        try:
            tee_stdout.write("Tk() inicializálása...\n")
            root = tk.Tk()
            tee_stdout.write("Tk() létrehozva\n")
            
            tee_stdout.write("VideoEncoderGUI inicializálása...\n")
            app = VideoEncoderGUI(root)
            tee_stdout.write("VideoEncoderGUI létrehozva\n")
            tee_stdout.write(f"📁 SQLite adatbázis fájl: {app.db_path}\n")
            
            if short_test:

                def _short_test_exit():

                    msg = "\n=== SHORTTEST mód: 10 másodperc eltelt, automatikus kilépés ===\n"

                    try:

                        tee_stdout.write(msg)

                    except Exception:

                        print(msg)

                    try:

                        root.quit()

                    except Exception:

                        pass

                    try:

                        root.destroy()

                    except Exception:

                        pass

                root.after(10_000, _short_test_exit)

            

            tee_stdout.write("mainloop() indítása...\n")
            root.mainloop()
            tee_stdout.write("mainloop() befejezve\n")
        except Exception as e:
            tee_stderr.write(f"HIBA: {e}\n")
            import traceback
            traceback.print_exc(file=tee_stderr)
        finally:
            log_file.close()
    else:
        main(short_test=short_test)



