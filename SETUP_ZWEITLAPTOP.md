# Setup: Repo auf Zweitlaptop synchronisieren

## Einmaliges Setup

### 1. GitHub Account erstellen
- Gehe zu https://github.com und erstelle einen Account (falls nicht vorhanden)

### 2. Privates Repo erstellen
- GitHub → "New repository" (grüner Button oben rechts)
- Name: `feed-collector` (oder beliebig)
- **Private** auswählen
- Ohne README erstellen (alles leer lassen)
- "Create repository" klicken

### 3. Personal Access Token erstellen
- GitHub → Profilbild → Settings → Developer settings → Personal access tokens → Tokens (classic)
- "Generate new token (classic)"
- Name: z.B. "laptop-sync"
- Scope: nur **repo** anhaken
- "Generate token" → Token kopieren und sicher speichern!

### 4. Hauptlaptop: Code hochladen
Terminal im Projektordner öffnen:
```bash
git init
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin https://github.com/DEIN-USERNAME/feed-collector.git
git push -u origin main
```
Bei Passwort-Abfrage: **Token** eingeben (nicht GitHub-Passwort).

### 5. Zweitlaptop: Repo klonen
```bash
git clone https://github.com/DEIN-USERNAME/feed-collector.git
cd feed-collector
```

---

## Bei Änderungen

### Hauptlaptop (nach Code-Änderungen):
```bash
git add .
git commit -m "Beschreibung der Änderung"
git push
```

### Zweitlaptop (Updates holen):
```bash
git pull
```

---

## Typischer Workflow

1. Code auf Hauptlaptop ändern
2. `git add . && git commit -m "xyz" && git push`
3. Auf Zweitlaptop: `git pull`
4. Falls Docker läuft: Container neu starten
   ```bash
   docker-compose -f docker-compose.feeds.yml down
   docker-compose -f docker-compose.feeds.yml up -d
   ```
