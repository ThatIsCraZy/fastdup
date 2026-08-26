# fastdup im Vergleich zu Data Domain und StoreOnce

Status: Recherchegrundlage, keine Architekturentscheidung oder
Produktzertifizierung  
Zuletzt geprüft: 2026-08-26

## Fragestellung

Welche Unterschiede zwischen dem fastdup-Prototyp und den kommerziellen
Backup-Plattformen Dell PowerProtect Data Domain und HPE StoreOnce lassen sich
für die README sachlich belegen?

Der Vergleich beschreibt Produktgrenzen und dokumentierte Funktionen. Er ist
kein unabhängiger Leistungs-, Kosten- oder Datenreduktionsvergleich.

## Kurzfazit

fastdup ist ein Apache-2.0-lizenzierter, experimenteller Single-Node-
POSIX-Speicher für Linux. Sein eigener Schwerpunkt ist die transparente,
bytegenaue Speicherung eines normalen Datei-Namensraums mit inhaltsabhängigem
Chunking, exakter globaler Deduplizierung, optionaler Zstd-Kompression,
unveränderlichen Containern, generationenbasierten Checkpoints und
neu aufbaubaren Beschleunigungsindizes. Die Format-, Recovery- und
GC-Invarianten sind als Quellcode, Spezifikationen und ADRs einsehbar.
[Projektkontext](../../CONTEXT.md),
[MVP-Entscheidung](../adr/0032-deliver-a-posix-exact-dedup-mvp-before-advanced-reduction.md),
[Apache-2.0-Metadaten](../../Cargo.toml)

Dell und HPE positionieren ihre Systeme dagegen als kommerzielle, speziell für
Backup und Recovery entwickelte Plattformen. Sie verbinden Deduplizierung mit
herstellerspezifischen Backup-Protokollen, breiten Softwareintegrationen,
Replikation, Cloud-Tiering, Sicherheits- und Immutability-Funktionen,
zentralem Multi-System-Management sowie Hersteller-Support. Data Domain und
StoreOnce unterstützen daneben auch herkömmliche NAS-Zugänge. fastdup ist
deshalb derzeit kein funktionsgleicher Ersatz für diese Produkte.
[Dell Produktübersicht](https://www.dell.com/en-us/dt/data-protection/powerprotect-backup-appliances/powerprotect-dd-backup-appliances.htm),
[HPE StoreOnce QuickSpecs](https://www.hpe.com/us/en/collaterals/collateral.c04328820.html)

## Belegbarer Funktionsvergleich

| Bereich | fastdup, aktueller Prototyp | Dell PowerProtect Data Domain | HPE StoreOnce |
| --- | --- | --- | --- |
| Produktgrenze | Linux-Software und FUSE/POSIX-Namensraum auf einem Knoten; Hardware- und Blockredundanz bleiben außerhalb des Projekts. | Purpose-built Backup- und Recovery-Appliances sowie eine softwaredefinierte Virtual Edition für On-Premises- und Cloud-Umgebungen. | Purpose-built Appliances und VSA für Backup und Recovery. |
| Datenzugriff | Normale POSIX-Dateioperationen; SMB ist über Samba vorgesehen. Kein proprietäres Backup-Client-Protokoll erforderlich. | DD Boost sowie gleichzeitig NFS, CIFS/SMB und VTL; Integrationen mit Dell- und Drittanbieter-Backupsoftware. | StoreOnce Catalyst, VTL, NFS und SMB; Integrationen mit verbreiteter Backupsoftware. |
| Datenreduktion | Inhaltsabhängige Chunks, exakte Deduplizierung und RAW/Zstd-Auswahl; noch keine allgemeine Reduktionszusage. | Dell bewirbt die aktuellen großen Systeme mit typischerweise 75:1 Datenreduktion; Dell kennzeichnet dies als internes Test-/Telemetrieergebnis und weist darauf hin, dass reale Ergebnisse variieren. | HPE berechnet die beworbene effektive Kapazität mit 60:1 aus Deduplizierung und Kompression und nennt Datentyp, Änderungsrate, Backupplan und Retention als Einflussfaktoren. |
| Replikation und Cloud | Nicht Bestandteil des MVP; kann nur außerhalb fastdups auf Block-/Dateisystem- oder Anwendungsebene gebaut werden. | Integrierte Replikation; DD Cloud Tier verschiebt Daten zur Langzeitaufbewahrung zu unterstützten Cloud-/Object-Storage-Zielen. | StoreOnce Replication und Catalyst Copy; Cloud Bank Storage nutzt externen Object Storage für Langzeitaufbewahrung. |
| Schutzfunktionen | Byte- und Strukturprüfungen, absturzsichere Generationen, Recovery und Scrub; kein zertifiziertes WORM/Compliance-Produkt, keine integrierte Verschlüsselung und kein Hardware Root of Trust. | Retention Lock in Governance- und Compliance-Modi, Datenverschlüsselung, Secure Boot und Hardware Root of Trust gehören zum dokumentierten Sicherheitsangebot. | Verschlüsselung at rest/in flight, Secure Erase und Catalyst Data Immutability; HPE dokumentiert server- und ISV-gesteuerte Retention sowie die empfohlenen Rollentrennungen. |
| Mehrere Systeme | Ein einzelner Writer/ein Knoten; kein verteilter Namespace und kein eingebautes Scale-out. | DDMC verwaltet mehrere Systeme. Smart Scale gruppiert Systeme in Pools und stellt DD-Boost-Clients mobile Storage Units über Namespace-Redirection bereit; dies ist nicht mit einem beliebigen POSIX-Scale-out-Dateisystem gleichzusetzen. | StoreOnce Federation verwaltet bis zu 40 Systeme über eine Oberfläche; die offizielle Beschreibung ist zentrales Management, nicht automatisch ein einziger verteilter POSIX-Namensraum. |
| Betrieb | Community-/Eigenbetrieb ohne Produkt-SLA, Zertifizierung, Appliance-Lifecycle oder Hersteller-Eskalation. | Kommerzielles Produkt mit Dell-Lizenzierung, Supportportal, Service Requests sowie dokumentiertem Hardware-/Software-Lifecycle. | Kommerzielles Produkt; die aktuellen QuickSpecs nennen für die Appliance-Modelle drei Jahre Teileaustausch, Arbeit und Vor-Ort-Support mit Reaktion am nächsten Arbeitstag zu normalen Geschäftszeiten. |

## Offizielle Herstellerquellen

### Dell PowerProtect Data Domain

- Dell bezeichnet Data Domain als zweckgebundene Backup- und Recovery-Systeme
  und nennt Immutability, Verschlüsselung, Hardware Root of Trust, Secure Boot,
  ein Backupsoftware-Ökosystem und Multicloud als Plattformmerkmale.
  [Data-Domain-Produktseite](https://www.dell.com/en-us/dt/data-protection/powerprotect-backup-appliances/powerprotect-dd-backup-appliances.htm)
- Die aktuelle Portfolioseite führt Hardware-Appliances und die
  softwaredefinierte Data Domain Virtual Edition auf. Die dortigen logischen
  Kapazitäten beruhen je nach Modell auf bis zu 50-facher Deduplizierung oder
  75:1 Datenreduktion; Dell weist auf variablen DDOS-Overhead hin.
  [Data-Domain-Portfolio und Kapazitäten](https://www.dell.com/en-us/shop/powerprotect-data-domain/sf/powerprotect-data-domain)
- DD-Systeme können DD Boost, NFS, CIFS und DD VTL gleichzeitig bereitstellen.
  [DDOS: Data access by protocol](https://www.dell.com/support/manuals/en-tv/dd-os-7.10/dd_p_ddos_7.10.1.70_ag/data-access-by-protocol?guid=guid-ff3483b7-d324-4cc5-8814-877818407dfd&lang=en-us)
- DD Retention Lock unterstützt je nach Zugriffsprotokoll Governance- und
  Compliance-Modi; die Protokollmatrix und Einschränkungen sind Teil der
  offiziellen DDOS-Dokumentation.
  [DDOS: Retention-Lock-Protokolle](https://www.dell.com/support/manuals/en-ca/dd-os-7.10/dd_p_ddos_7.10.1.70_ag/supported-data-access-protocols?guid=guid-c3a7d2ce-e71f-4562-a176-d0f08daadd16&lang=en-us)
- DD Cloud Tier ist für Langzeitaufbewahrung ausgelegt und bewegt Daten aus dem
  Active Tier zu externen Cloud-Providern.
  [Dell NetWorker/Data Domain Cloud Tier](https://www.dell.com/support/manuals/en-us/networker/nw_p_ddboost_int_guide_19.9/data-domain-cloud-tier?guid=guid-74cb017a-5c91-4947-b481-e5aa5e286003&lang=en-us)
- Smart Scale bildet aus mehreren DD-Systemen einen logischen System-Pool und
  abstrahiert für unterstützte DD-Boost-Workflows die Platzierung mobiler
  Storage Units. Die dokumentierte DDMC-Version unterstützt dabei bis zu 32
  Appliances pro Rechenzentrum, mit Modell-, Software- und
  Backupanwendungsgrenzen.
  [DDMC Smart Scale](https://www.dell.com/support/manuals/en-us/idp-other/dd_p_ddmc_user_guide/smart-scale?guid=guid-7a645403-05fe-4d8a-b83f-b8e467eeac03)

### HPE StoreOnce

- Die aktuellen QuickSpecs führen StoreOnce Catalyst, VTL, NFS und SMB als
  Zieltypen auf. Sie dokumentieren Hardwaremodelle, Kapazitäten,
  Leistungsgrenzen, Replikation, Verschlüsselung, Secure Erase, Cloud Bank,
  Federation und Support.
  [HPE StoreOnce QuickSpecs](https://www.hpe.com/us/en/collaterals/collateral.c04328820.html)
- HPE beschreibt Catalyst als backup-optimiertes Protokoll, über das die
  Backupanwendung Backups, Kopien und Replikation steuert. Cloud Bank erweitert
  Catalyst um externen Object Storage für langfristige Aufbewahrung.
  [HPE StoreOnce Produktseite](https://www.hpe.com/us/en/storage/storeonce.html)
- Die QuickSpecs leiten effektive Kapazität aus angenommenen 60:1 Einsparungen
  durch Deduplizierung und Kompression ab und sagen ausdrücklich, dass die reale
  Einsparung vom Workload und der Aufbewahrungspolitik abhängt.
  [HPE StoreOnce QuickSpecs](https://www.hpe.com/us/en/collaterals/collateral.c04328820.html)
- Catalyst Data Immutability verhindert Änderung oder Löschung während einer
  Mindestaufbewahrungszeit. Die Dokumentation unterscheidet ISV- und
  servergesteuerte Immutability. Bei servergesteuerter Immutability kann ein
  StoreOnce-Administrator den Store weiterhin löschen; HPE empfiehlt daher
  Administratortrennung und Dual Authorization.
  [HPE Catalyst Store properties](https://support.hpe.com/hpesc/public/docDisplay?docId=sd00002924en_us&docLocale=en_US&page=GUID-79BE400A-633B-4CD3-81BF-98AE936FC7B0.html)
- Eine StoreOnce Federation ermöglicht zentrales Management von bis zu 40
  Systemen. Die Systeme bleiben Lead- und Member-Systeme; Member müssen nicht
  untereinander kommunizieren.
  [HPE Federation guidelines](https://support.hpe.com/hpesc/public/docDisplay?docId=sd00002326en_us&docLocale=en_US&page=GUID-117E986F-18CA-4EDD-AD85-0C6C7D42D9E9.html)

## Formulierungen, die in der README sicher sind

- „fastdup reduziert Daten transparent innerhalb eines normalen
  POSIX-Namensraums und ist als offener, hardwareunabhängiger Linux-
  Speicherbaustein konzipiert.“
- „Data Domain und StoreOnce sind kommerzielle Backup-Plattformen mit
  proprietären Beschleunigungsprotokollen, breiten Backupsoftware-
  Integrationen sowie Replikations-, Cloud-, Security- und Support-Angeboten;
  fastdup deckt diese Produktgrenze heute nicht ab.“
- „Der Unterschied liegt nicht in einem pauschalen Anspruch auf bessere
  Deduplizierung, sondern in Offenheit, POSIX-Transparenz und einer bewusst
  kleineren, nachvollziehbaren Architektur.“
- „Wer Hersteller-Support, zertifizierte Immutability, integrierte
  Verschlüsselung, Replikation, Cloud-Tiering, HA oder validierte
  Backupsoftware-Matrizen braucht, benötigt heute eine kommerzielle Plattform
  oder zusätzliche Komponenten.“

## Nicht belegbare oder irreführende Aussagen

Die README sollte ohne unabhängige Vergleichsmessung **nicht** behaupten:

- fastdup dedupliziere oder komprimiere besser als Data Domain oder StoreOnce;
- fastdup sei schneller, billiger oder energieeffizienter;
- ein Herstellerverhältnis wie 75:1 oder 60:1 sei eine garantierte oder
  workloadunabhängige Eigenschaft;
- normaler SMB-/POSIX-Zugriff sei bei den kommerziellen Systemen unmöglich;
- offen oder softwaredefiniert bedeute automatisch geringere Gesamtbetriebskosten;
- die aktuelle fastdup-Integritätsarchitektur sei gleichwertig mit
  zertifizierter Compliance-Immutability, HA, Geräteredundanz oder
  Hersteller-Support; oder
- Data Domain Smart Scale beziehungsweise StoreOnce Federation seien
  unmittelbar mit einem verteilten POSIX-Dateisystem gleichzusetzen.

