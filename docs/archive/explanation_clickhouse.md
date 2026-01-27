1. Wie ClickHouse mit Daten umgeht (nochmals ganz verständlich)

Du hast ein laufendes Programm (den ClickHouse-Server) – das ist wie eine „Zentrale“, die alle Daten verwaltet.
Es läuft auf deinem Computer (oder in Docker).

Deine Python-Skripte oder dein Terminal reden mit diesem Server.

Du kannst also Daten per Python einfügen (INSERT INTO ...)

oder per Terminal abfragen (SELECT ...).

ClickHouse speichert die Daten intern auf deiner Festplatte.

Nicht als eine Datei wie trades.db, sondern als viele kleine Dateien,
die ClickHouse selbst verwaltet (z. B. Spalten getrennt, komprimiert, indexiert).

Der Speicherort hängt vom Installationsweg ab (gleich dazu mehr).

Wenn du die Daten sichern, verschieben oder archivieren willst,
hast du zwei Möglichkeiten:

Den gesamten ClickHouse-Datenordner kopieren (raw backup, für denselben Server).

Oder deine Daten exportieren in ein neutrales Format wie Parquet oder CSV,
das du z. B. in AWS S3, Google Cloud oder auf einer externen Festplatte speichern kannst.