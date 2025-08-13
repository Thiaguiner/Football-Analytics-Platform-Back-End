import polars as pl
import os, re, glob, time, io, json

from flask import Flask

class Model:
    def __init__(self):
        pass
    
    def tabela_anual(self, ano):  
        df_brasileirao = pl.read_parquet(fr'C:\Users\Thiago_SL\projeto-backend\Football-Analytics-Platform-Back-End\brasileirao.parquet')
        df_campbras = pl.read_parquet(fr'C:\Users\Thiago_SL\projeto-backend\Football-Analytics-Platform-Back-End\campbras.parquet')
        if ano > 2023 or ano < 2003:
            return {"content": []}
        else:
            df_brasileirao = df_brasileirao.with_columns(pl.when(pl.col("team") == 'SaoPaulo').then("São Paulo").when(pl.col("team") == "Atletico-MG").then("Atlético-MG").when(pl.col("team") == "America-MG").then("América-MG").when(pl.col("team") == 'Goias').then("Goiás").when(pl.col("team") == 'RBBragantino').then("Bragantino").when(pl.col("team") == 'Cuiaba').then('Cuiabá').when(pl.col("team") == 'Ceara').then('Ceará').when(pl.col("team") == "Atletico-GO").then("Atlético-GO").when(pl.col("team") == "Avai").then("Avaí").otherwise(pl.col("team")).alias("team"))
            df_brasileirao = df_brasileirao.with_columns(pl.when(pl.col("team") == 'SaoPaulo').then("São Paulo").when(pl.col("team") == "Atletico-MG").then("Atlético-MG").when(pl.col("team") == "America-MG").then("América-MG").when(pl.col("team") == 'Goias').then("Goiás").when(pl.col("team") == 'RBBragantino').then("Bragantino").when(pl.col("team") == 'Cuiaba').then('Cuiabá').when(pl.col("team") == 'Ceara').then('Ceará').when(pl.col("team") == "Atletico-GO").then("Atlético-GO").when(pl.col("team") == "Avai").then("Avaí").when(pl.col("team") == 'Gremio').then("Grêmio").when(pl.col("team") == 'Nautico').then("Náutico").when(pl.col("team") == "America-RN").then("América-RN").when(pl.col("team") == "Parana").then("Paraná").when(pl.col("team") == 'Vitoria').then('Vitória').when(pl.col("team") == 'SantoAndre').then("Santo André").when(pl.col("team") == 'PontePreta').then("Ponte Preta").when(pl.col("team") == 'Criciuma').then('Criciúma').when(pl.col("team") == 'SantaCruz').then("Santa Cruz").otherwise(pl.col("team")).alias("team"))
            
            df_brasileirao_tabela = df_brasileirao.filter(pl.col("season") == ano)

            df_filtrado = df_campbras.filter(pl.col("year") == ano)

            gols_mandante = (df_filtrado.groupby("hometeam").agg(pl.col("goalsht").sum().alias("goalsht")).rename({"hometeam":"team"}))            
            gols_visitantes = (df_filtrado.groupby("visitingteam").agg(pl.col("goalsvt").sum().alias("goalsvt")).rename({"visitingteam":"team"}))  
            gols_totais = (gols_mandante.join(gols_visitantes, on="team", how="outer").fill_null(0).with_columns((pl.col("goalsht") + pl.col("goalsvt")).alias("total_goals")))
            
            gols_levados_mandante =(df_filtrado.groupby("hometeam").agg(pl.col("goalsvt").sum().alias("gols_sofridos_visitantes")).rename({"hometeam":"team"}))
            gols_levados_visitantes = (df_filtrado.groupby("visitingteam").agg(pl.col("goalsht").sum().alias("gols_sofridos_mandantes")).rename({"visitingteam":"team"}))
            gols_levados_totais = (gols_levados_mandante.join(gols_levados_visitantes, on="team", how="outer").fill_null(0).with_columns((pl.col("gols_sofridos_visitantes") + pl.col("gols_sofridos_mandantes")).alias("gols_tomados_totais")))
            
            df_brasileirao_tabela = (df_brasileirao_tabela
                         .join(gols_totais.select(["team", "total_goals"]), on="team", how="left")
                         .join(gols_levados_totais.select(["team", "gols_tomados_totais"]), on="team", how="left")
                         .with_columns((pl.col("total_goals") - pl.col("gols_tomados_totais")).alias("goal_difference")))
            
            # df_brasileirao_tabela = (df_brasileirao_tabela.join(gols_totais.select(["team", "total_goals"]), on="team", how="left").join(gols_levados_totais.select(["team", "gols_tomados_totais"]), on="team", how="left"))
            
            print(df_brasileirao_tabela)
        return {"content": df_brasileirao_tabela.to_dicts()}
    
    def info_time(self, time):
        df_teams = pl.read_parquet(fr'C:\Users\Thiago_SL\projeto-backend\Football-Analytics-Platform-Back-End\teams.parquet')
        df_teams = df_teams.with_columns(pl.when(pl.col("team") == 'SaoPaulo').then("São Paulo").when(pl.col("team") == "Atletico-MG").then("Atlético-MG").when(pl.col("team") == "America-MG").then("América-MG").when(pl.col("team") == 'Goias').then("Goiás").when(pl.col("team") == 'RBBragantino').then("Bragantino").when(pl.col("team") == 'Cuiaba').then('Cuiabá').when(pl.col("team") == 'Ceara').then('Ceará').when(pl.col("team") == "Atletico-GO").then("Atlético-GO").when(pl.col("team") == "Avai").then("Avaí").when(pl.col("team") == 'Gremio').then("Grêmio").when(pl.col("team") == 'Nautico').then("Náutico").when(pl.col("team") == "America-RN").then("América-RN").when(pl.col("team") == "Parana").then("Paraná").when(pl.col("team") == 'Vitoria').then('Vitória').when(pl.col("team") == 'SantoAndre').then("Santo André").when(pl.col("team") == 'PontePreta').then("Ponte Preta").when(pl.col("team") == 'Criciuma').then('Criciúma').when(pl.col("team") == 'SantaCruz').then("Santa Cruz").otherwise(pl.col("team")).alias("team"))
        
        df_teams = df_teams.with_columns(pl.when(pl.col("region") == 'Southeast').then("Sudeste").when(pl.col("region") == 'Northeast').then("Nordeste").when(pl.col("region") == 'North').then("Norte").when(pl.col("region") == 'South').then("Sul").when(pl.col("region") == 'Central').then("Centro-Oeste").otherwise(pl.col("region")).alias("region"))
        print(df_teams)
        df_teams = df_teams.filter(pl.col("team") == time)
        
        
    
        return {"content": df_teams.to_dicts()}
    