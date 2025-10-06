## ⚙️ Development Environment Setup

To set up and run the code in your development environment, follow the standard project installation steps and include the local certificate trust step as described below.

---

### **1. Dependencies Installation**

Follow the standard installation steps exactly as outlined in the project's main `README.md` file.

---

### **2. Local HTTPS Trust Integration**

After successfully running the main dependency installation command ( `poetry install`), you must run the following script **before activating the virtual environment**.  
This step is crucial for enabling **local HTTPS communication**.

**Action:** Run the mkcert root CA integration script:

```bash
sh setup-mkcert-ca.sh
```
This script `setup-mkcert-ca.sh`  modifies the certifi bundle file after it's been installed, ensuring Python will trust local HTTPS certs before the environment is activated.

A CA bundle, short for Certificate Authority bundle, is a text file that contains a collection of trusted root and intermediate public certificates from various Certificate Authorities (CAs)