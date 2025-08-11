import polars as pl
import os, re, glob, time, io, json

from flask import Flask

class TabelaModel:
    def __init__(self):
        pass
    
    def tabela_anual(self, ano):  
        print("ano", ano)
        return {"ano": ano}  