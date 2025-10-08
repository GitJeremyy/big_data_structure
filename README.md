## Utilisation

1. **Clone le repo**

```bash
git clone <repository-url>
cd big_data_structure
```

2. **Installer UV (si nécessaire)**

```bash
pip install uv
```

ou

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

3. **Installer les dépendances**

```bash
uv sync
```

4. **Créer et démarrer l'environnement virtuel**

```bash
uv venv --python 3.13
.\.venv\Scripts\Activate
```

5. **Démarrage de l'API**

```bash
uv run fastapi dev app/main.py
```
