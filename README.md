# Mycorr-python

##  Dependencies Installation

1.  **Install Poetry (if missing):**
    ```bash
    curl -sSL https://install.python-poetry.org | python3 -

    ```

2.  **Add Poetry to your PATH:**
    ```bash
    export PATH="$HOME/.local/bin:$PATH"
    ```

3.  **Verify installation:**
    ```bash
    poetry --version
    ```

4.  **Install project dependencies:**
    ```bash
    poetry install
    ```

5.  **Add Poetry shell plugin:**
    ```bash
    poetry self add poetry-plugin-shell
    ```

---

6.  **Activate virtual environment:**
    ```bash
    poetry shell
    ```
### Virtual environment activation :

Run this command the first time only to register the environment as a Jupyter kernel:

```bash 
python -m ipykernel install --user --name=mycorr --display-name "Python (mycorr)"
 ```

---

### Usage :
```bash 
poetry run jupyter notebook
```

Then:

- Open **main.ipynb**
- Select **Python (mycorr)** kernel
- All set, run the cells!
