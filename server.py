from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # Libera acesso de outros domínios (como o Angular)


@app.route("/api/hello", methods=["GET"])
def hello():
    return jsonify({"message": "Olá do Flask!"})


if __name__ == "__main__":
    app.run(debug=True, port=5000)
