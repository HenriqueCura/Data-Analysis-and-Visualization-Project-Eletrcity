import pyttsx3, PyPDF2 
from pathlib import Path
import sys
from agent import run_agent

if len(sys.argv) != 2:
    print(f"Usage: {sys.argv[0]} <path> ")
    sys.exit(1)


def carrega_pdf(path: str):
    try:
        path = Path(path)
        pdf = PyPDF2.PdfReader(path)
        return pdf
    except FileNotFoundError:
        print('Ficheiro não existe!')
        sys.exit(1)

def speaker_init():
    speaker = pyttsx3.init(); return speaker

def obtem_texto(pdf):
    texto_inteiro = []
    for pag_n in range(len(pdf.pages)):
        text = pdf.pages[pag_n].extract_text()
        clean_text = text.strip().replace('/n',' ')
        texto_inteiro.append(clean_text)
    full_content = " ".join(texto_inteiro)

    # ESTRATÉGIA: Cortar as referências
    # Tenta encontrar o índice da última ocorrência de "References"
    marcador_fim = full_content.rfind("References")
    if marcador_fim == -1:
        marcador_fim = full_content.rfind("REFERENCES")
    
    if marcador_fim != -1:
        # Mantém apenas o texto antes das referências
        full_content = full_content[:marcador_fim]

    return full_content

def main():
    path = sys.argv[1]
    pdf = carrega_pdf(path)
    speaker = speaker_init()
    clean_text = obtem_texto(pdf)
    resumo = run_agent(clean_text)
    pathII = f'{path[:-4]}.mp3' # nome padrão para cada ficheiro sendo retirado o .pdf
    speaker.save_to_file(resumo,pathII)
    speaker.runAndWait()
    speaker.stop()

if __name__ == '__main__':
    main()