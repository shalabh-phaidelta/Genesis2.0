from dotenv import load_dotenv

load_dotenv()

import uvicorn

from wm_server import api


if __name__ == '__main__':
    print("Runing host server")
    uvicorn.run(api.app, host='0.0.0.0', port=5000)
