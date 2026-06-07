"""Punto de entrada del dashboard Streamlit (placeholder, sin logica de negocio).

Navegacion entre 4 pantallas (ver Architecture.md, seccion 9):
Macro Argentina, Mercados globales, Correlaciones, AI Analyst.
"""

import streamlit as st


def main() -> None:
    """Renderiza la pagina principal (placeholder)."""
    st.set_page_config(page_title="Macro Intelligence Pipeline", layout="wide")
    st.title("Macro Intelligence Pipeline")
    st.info("Placeholder. El dashboard aun no esta implementado.")


if __name__ == "__main__":
    main()
