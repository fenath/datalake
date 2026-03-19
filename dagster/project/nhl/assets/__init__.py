# project/crypto_candles/assets/__init__.py
import dagster as dg

# crypto_assets = load_assets_from_package_module(__name__)
nhl_assets = dg.load_assets_from_package_name(
    __name__, 
    group_name="nhl_domain" # Todos os assets desse pacote aparecerão juntos na UI
)

# TESTE DE COMENTARIO
