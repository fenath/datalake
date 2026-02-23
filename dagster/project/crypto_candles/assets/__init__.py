# project/crypto_candles/assets/__init__.py
from dagster import load_assets_from_package_module, load_assets_from_current_module, load_assets_from_package_name

from . import raw, bronze, silver

# crypto_assets = load_assets_from_package_module(__name__)
crypto_assets = load_assets_from_package_name(
    __name__, 
    group_name="crypto_domain" # Todos os assets desse pacote aparecerão juntos na UI
)

# TESTE DE COMENTARIO
