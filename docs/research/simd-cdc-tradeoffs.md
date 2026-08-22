# SIMD-freundliche CDC-Algorithmen und Dedupe-Kosten

Stand: 2026-08-22. Diese Notiz trennt Grenzfindungs-Durchsatz von
Dedup-Ergebnis. Sie bewertet keine neue Production-Policy. fastdup nutzt
FastCDC-v1 mit 16 KiB Minimum, 64 KiB Ziel und 256 KiB Maximum. Eine andere
Grenzfunktion erzeugt andere Chunk IDs und braucht nach [ADR 0014](../adr/0014-allow-chunking-profiles-per-data-region.md)
ein neues versioniertes Profil.

## Vergleichbare Fakten

| Verfahren | veröffentlichter Durchsatzbefund | veröffentlichter Dedupe-Befund | Vergleichbarkeit mit 16/64/256 KiB |
| --- | --- | --- | --- |
| FastCDC | ATC'16 meldet rund 10x gegenüber dem dort getesteten Rabin-CDC und rund 3x gegenüber Gear und AE. | Bei 8-KiB-Erwartungsgröße und 2-KiB-Minimum liegt FastCDC in allen sieben Tabellen-Corpora nahe Rabin. Mit Normalisierung und 4 KiB Minimum übertrifft es Rabin in mehreren dieser Zahlen. | Nur qualitativ. Die Studie nutzt 8 KiB erwartet, 2 bis 8 KiB Minimum und 64 KiB Maximum, nicht fastdups Profil. |
| SS-CDC auf Gear oder CRC | Der VectorCDC-Vergleich misst mit AVX-512 und 4 bis 16 KiB 3,13x für Gear und 2,58x für CRC. | SS-CDC beansprucht identische Grenzen zur jeweiligen sequentiellen Referenz. | Der Mechanismus ist für FastCDC ungeeignet, siehe nächste Zeile. |
| SS-CDC auf FastCDC | Der VectorCDC-Vergleich beobachtet keinen Gewinn. | Die Grenzfunktion bleibt nur dann gleich, wenn sie die FastCDC-Regeln vollständig nachbildet. | Direkt relevant. Das Profil hat ein Viertel der Zielgröße als Minimum und gewinnt damit viel durch Sub-Minimum-Skipping. |
| VectorCDC auf RAM, AE und MAXP | Das Manuskript meldet 5,51 bis 17,6x gegen die jeweiligen unbeschleunigten Algorithmen und 8,35 bis 26,2x gegen SS-CDC-Varianten. | Bei 8-KiB-Zielgröße liegt der beste hashlose Algorithmus über Corpora und Größen höchstens 11 Prozent unter dem besten hashbasierten. AE-Min fällt auf MAPS auf 8,89 Prozent Space Savings, während andere CDCs 58 bis 78 Prozent erreichen. | Nicht kompatibel. RAM, AE und MAXP sind andere Grenzfunktionen. 8-KiB-Graphen sagen nichts Verlässliches über 64 KiB. |
| SeqCDC | Der TPDS-Preprint meldet rund 10x gegen nicht vektorisiertes CDC und 1,2 bis 1,35x gegen vektorisiertes CDC. | Über alle gemessenen Corpora und Größen liegt SeqCDC laut Autoren höchstens 6 Prozent unter dem jeweils besten Verfahren. Bei 16 KiB auf RDS misst es 91,13 Prozent, FastCDC 92,50 Prozent. Bei 16 KiB auf TPCC misst SeqCDC 85,72 Prozent, FastCDC 86,26 Prozent. | Nicht kompatibel. Die Zahlen sind bei 16 KiB, nicht 64 KiB, und beschreiben eine neue hashlose Policy. |
| RapidCDC | SoCC'19 meldet bis zu 33x Chunking-Speedup gegenüber normalem CDC. | Nahezu gleiche Dedupe-Rate, solange sein Duplicate-Locality-Prädiktor passende Folgegrenzen findet. | Interessant für spätere Exact-Reuse-Arbeit, aber kein SIMD-CDC und kein Gewinn für neue, nichtduplizierbare Bytes. |
| P-Dedupe | 3 bis 4x auf einem Quad-Core-i7 für die gesamte CDC-Dedupe-Pipeline. | Etwa 0,02 Prozentpunkte Dedupe-Verlust über acht Datensätze und Rabin, Adler und Gear. | Nicht zulässig als FastCDC-v1-Implementierung. Segment-Rechunking ist keine identische Grenzfolge. |

## FastCDC gegen SIMD-Entkopplung

Der wichtige negative Befund ist klarer als jede große SIMD-Zahl. FastCDC
überspringt die Bytes vor der Mindestgröße. SS-CDC berechnet zuerst eine
globale Kandidaten-Bitmap und kann diese Optimierung dann nicht nutzen. Die
VectorCDC-Autoren beobachteten deshalb keinen Durchsatzgewinn für FastCDC,
während sie Gear und CRC beschleunigten. Der Test nutzte AVX-512, zufällige
Daten und 4, 8 und 16 KiB Chunkgrößen. Er beweist keinen Wert für fastdups
64-KiB-Ziel, aber die Ursache ist direkt übertragbar: Das 16-KiB-Minimum macht
das Auslassen der ersten 25 Prozent einer Zielchunkgröße ausdrücklich Teil der
FastCDC-v1-Policy.

SS-CDC, MUCH und P-Dedupe sind daher keine Drop-in-Optimierungen. SS-CDC und
MUCH lösen das Korrektheitsproblem für ihre eigenen Rolling-Hash-Regeln.
P-Dedupe akzeptiert einen kleinen Dedupe-Verlust. Keiner dieser Ansätze
publiziert einen FastCDC-v1-Gleichheitsbeweis für Gear-Tabelle,
Normalisierungsmaske und 16/64/256-KiB-Grenzen.

## Dedupe, Index und Metadaten sind getrennte Kosten

Space Savings oder Dedup-Ratio zählt nur gespeicherte Nutzdaten. Sie misst
weder Exact-Index-RAM, Run-Größe noch Manifest- oder Container-Overhead. Die
zitierten SIMD-Papers veröffentlichen dafür keinen direkt übertragbaren
fastdup-Wert.

Chunkgröße beeinflusst diese Kosten trotzdem unmittelbar. SeqCDC misst beim
Übergang von 4 auf 16 KiB in DEV, DKR, RDS und TPCC nur 0,5 bis 6 Prozent
weniger Space Savings und zugleich deutlich weniger Fingerprint-DB- und
Fingerprinting-Arbeit. Für LNX kostet derselbe Übergang dagegen 29,87 Prozent
Space Savings. Daraus folgt nur die Richtung: größere Chunks senken
Indexeinträge und Metadaten pro logischem Byte, können aber je nach Corpus
stark schlechter deduplizieren. Es rechtfertigt keine Extrapolation auf 64
KiB und erst recht keine Aussage über fastdups Exact-Run-Format.

Für fastdup zählt zusätzlich die harte Maximalgröße. Veröffentlichte
Durchsatzdaten mit "8 KiB" meinen oft erwartete oder Zielgröße und verwenden
andere Min-/Max-Verhältnisse. FastCDC ATC'16 verwendete etwa 8 KiB erwartet
und 64 KiB Maximum. Die FastCDC-v1-Relation ist 16/64/256 KiB. Damit ändern
sich sowohl der Anteil übersprungener Bytes als auch Chunkanzahl,
Kompressionsregion-Füllung und Indexarbeit.

## Bewertung für fastdup

1. Keine VectorCDC-, SS-CDC- oder SIMD-Gear-Implementierung als
   SingleStream-Optimierung bauen. Der einzige aktuelle FastCDC-spezifische
   Messwert ist negativ, und jede nichtidentische Grenzfolge verletzt die
   v1-Policy.
2. SeqCDC, RAM und AE nicht unter "schnelleres CDC" einordnen. Sie sind neue
   Datenformate auf Chunk-Ebene. Ein späterer Kandidat braucht ein eigenes
   Profil, Golden-Corpus, Datenreduktions- und Index-/Metadatenmessung sowie
   Restore- und Rechunking-Migration.
3. RapidCDC ist der einzige hier betrachtete Ansatz, der zur vorhandenen
   Exact-Reuse-Richtung passt. Seine Chance liegt bei wiederholten Daten. Vor
   einem Versuch muss der Online-Proof-/Exact-Pfad die Folgegrenzen ohne
   Scannen beweisen können. Für den ersten, physischen ISO-Upload ist kein
   33x-Wert zu erwarten.
4. Bevor CDC geändert wird, den vorhandenen FastCDC-Scanner isoliert mit
   genau 16/64/256 KiB gegen BLAKE3, verbleibende Kopien, Exact-Lookup und
   Zstd messen. Die bisherigen End-to-End-Tests haben asynchronen CDC-Hash-
   Overlap bei zwei Streams bereits negativ bewertet.

## Promotion-Gate für eine neue CDC-Policy

Ein Vergleich muss je 1, 2 und 4 SMB-Streams abdecken und für alle Kandidaten
ausweisen: CDC-Bytes/s, Gesamtdurchsatz, Dedupe-Space-Savings,
`repository_allocated_bytes`, Chunkzahl, Chunkgrößenverteilung,
Exact-Index-Einträge und -Bytes, Manifest-/Containerbytes, CPU-Zeit, Peak-RSS,
p99 abgeschlossener Dateien sowie langsamsten Stream. Die Corpus-Matrix braucht
mindestens Rocky, VM-Backups, strukturierte Daten, komprimierte/verschlüsselte
Bytes und kleine Datei-Workloads. Ohne identische Samba-, FUSE-, CPU- und
Diskparameter sind die Durchsatzwerte nicht vergleichbar.

## Quellen

- [FastCDC, USENIX ATC 2016](https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf)
- [SS-CDC, Network and Internet Computing 2019](https://ranger.uta.edu/~sjiang/pubs/papers/ni19-ss-cdc.pdf)
- [VectorCDC-Manuskript mit FastCDC-Gegenbefund](https://cs.uwaterloo.ca/~alkiswan/papers/VectorCDC_TOS_2026.pdf)
- [SeqCDC TPDS-Preprint mit 16-KiB-Tabellen](https://sreeharshau.github.io/papers/VectorizedSeq_TPDS26.pdf)
- [RapidCDC, ACM SoCC 2019](https://ranger.uta.edu/~sjiang/pubs/papers/ni19-rapidcdc.pdf)
- [P-Dedupe, Future Generation Computer Systems 2019](https://www.sciencedirect.com/science/article/abs/pii/S0167739X18320053)
- [Google FastCDC-Implementierung und Parametersemantik](https://github.com/google/cdc-file-transfer/blob/main/fastcdc/fastcdc.h)
