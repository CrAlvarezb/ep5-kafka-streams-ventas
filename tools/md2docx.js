// Convierte un subconjunto de Markdown (titulos, parrafos, listas, codigo, tablas, citas) a .docx
// Uso: node md2docx.js salida.docx "Titulo portada" "Subtitulo" archivo1.md [archivo2.md ...]
const fs = require("fs");
const {
  Document, Packer, Paragraph, TextRun, HeadingLevel, Table, TableRow, TableCell, WidthType,
  ShadingType, BorderStyle, AlignmentType, LevelFormat, PageBreak, Footer, PageNumber,
  TableOfContents,
} = require("docx");

const [, , salida, tituloPortada, subtitulo, ...archivos] = process.argv;

const FUENTE = "Calibri";
const MONO = "Consolas";
const ANCHO = 9638; // A4 (11906) - margenes 2 x 1134
const AZUL = "1F3864";

// `codigo` dentro de un tramo de texto (que puede venir en negrita)
function conCodigo(texto, base) {
  const runs = [];
  const re = /`[^`]+`/g;
  let ultimo = 0, m;
  while ((m = re.exec(texto)) !== null) {
    if (m.index > ultimo) runs.push(new TextRun({ text: texto.slice(ultimo, m.index), font: FUENTE, ...base }));
    runs.push(new TextRun({ text: m[0].slice(1, -1), font: MONO, size: 19, color: "8B1A1A", ...base,
      shading: { type: ShadingType.CLEAR, fill: "F2F2F2", color: "auto" } }));
    ultimo = m.index + m[0].length;
  }
  if (ultimo < texto.length) runs.push(new TextRun({ text: texto.slice(ultimo), font: FUENTE, ...base }));
  return runs;
}

function inline(texto, base = {}) {
  // [txt](url) -> txt ; luego **negrita** (que puede contener `codigo`) y `codigo`
  texto = texto.replace(/\[([^\]]+)\]\([^)]+\)/g, "$1").replace(/\\\|/g, "|");
  const runs = [];
  const re = /\*\*(.+?)\*\*/g;
  let ultimo = 0, m;
  while ((m = re.exec(texto)) !== null) {
    if (m.index > ultimo) runs.push(...conCodigo(texto.slice(ultimo, m.index), base));
    runs.push(...conCodigo(m[1], { ...base, bold: true }));
    ultimo = m.index + m[0].length;
  }
  if (ultimo < texto.length) runs.push(...conCodigo(texto.slice(ultimo), base));
  return runs.length ? runs : [new TextRun({ text: "", font: FUENTE })];
}

function celdas(linea) {
  let l = linea.trim().replace(/\\\|/g, "\u0000");
  if (l.startsWith("|")) l = l.slice(1);
  if (l.endsWith("|")) l = l.slice(0, -1);
  return l.split("|").map((c) => c.replace(/\u0000/g, "|").trim());
}

function tabla(filas) {
  const enc = filas[0];
  const cuerpo = filas.slice(2);
  const n = enc.length;
  const largos = Array.from({ length: n }, (_, i) =>
    Math.min(60, Math.max(6, ...[enc, ...cuerpo].map((f) => (f[i] || "").replace(/[`*]/g, "").length))));
  const total = largos.reduce((a, b) => a + b, 0);
  const anchos = largos.map((l) => Math.max(900, Math.round((l / total) * ANCHO)));
  const ajuste = ANCHO - anchos.reduce((a, b) => a + b, 0);
  anchos[anchos.indexOf(Math.max(...anchos))] += ajuste;
  const borde = { style: BorderStyle.SINGLE, size: 4, color: "BFBFBF" };
  const bordes = { top: borde, bottom: borde, left: borde, right: borde };
  const fila = (f, esEnc) => new TableRow({
    tableHeader: esEnc,
    children: Array.from({ length: n }, (_, i) => new TableCell({
      width: { size: anchos[i], type: WidthType.DXA },
      borders: bordes,
      shading: esEnc ? { type: ShadingType.CLEAR, fill: AZUL, color: "auto" } : undefined,
      margins: { top: 60, bottom: 60, left: 100, right: 100 },
      children: [new Paragraph({ spacing: { before: 0, after: 0 },
        children: inline(f[i] || "", esEnc ? { bold: true, color: "FFFFFF", size: 19 } : { size: 19 }) })],
    })),
  });
  return new Table({
    width: { size: ANCHO, type: WidthType.DXA },
    columnWidths: anchos,
    rows: [fila(enc, true), ...cuerpo.map((f) => fila(f, false))],
  });
}

function bloqueCodigo(lineas) {
  const out = [];
  // Bloques anchos (salidas de terminal): letra mas chica para que cada linea quepa entera
  const tam = Math.max(0, ...lineas.map((l) => l.length)) > 92 ? 14 : 18;
  lineas.forEach((l, i) => out.push(new Paragraph({
    shading: { type: ShadingType.CLEAR, fill: "F3F3F3", color: "auto" },
    border: { left: { style: BorderStyle.SINGLE, size: 18, color: "2E75B6", space: 6 } },
    spacing: { before: i === 0 ? 80 : 0, after: i === lineas.length - 1 ? 120 : 0, line: 260 },
    indent: { left: 120 },
    children: [new TextRun({ text: l.length ? l : " ", font: MONO, size: tam })],
  })));
  return out;
}

function convertir(md, primero) {
  const out = [];
  const lineas = md.replace(/\r/g, "").split("\n");
  let i = 0, primerTitulo = true;
  while (i < lineas.length) {
    const l = lineas[i];
    if (l.startsWith("```")) {
      const buf = [];
      i++;
      while (i < lineas.length && !lineas[i].startsWith("```")) buf.push(lineas[i++]);
      i++;
      out.push(...bloqueCodigo(buf));
      continue;
    }
    if (/^\s*\|/.test(l) && i + 1 < lineas.length && /^\s*\|[\s:|-]+\|\s*$/.test(lineas[i + 1])) {
      const filas = [];
      while (i < lineas.length && /^\s*\|/.test(lineas[i])) filas.push(celdas(lineas[i++]));
      out.push(tabla(filas));
      out.push(new Paragraph({ spacing: { before: 0, after: 120 }, children: [] }));
      continue;
    }
    let m;
    if ((m = l.match(/^(#{1,4})\s+(.*)$/))) {
      const nivel = m[1].length;
      const txt = m[2].replace(/`/g, "");
      if (nivel === 1) {
        if (!primerTitulo || !primero) out.push(new Paragraph({ children: [new PageBreak()] }));
        out.push(new Paragraph({ heading: HeadingLevel.HEADING_1, children: [new TextRun({ text: txt })] }));
        primerTitulo = false;
      } else {
        out.push(new Paragraph({ heading: nivel === 2 ? HeadingLevel.HEADING_2 : HeadingLevel.HEADING_3,
          children: [new TextRun({ text: txt })] }));
      }
      i++;
      continue;
    }
    if (/^\s*[-*]\s+/.test(l)) {
      const nivel = Math.min(1, Math.floor((l.match(/^\s*/)[0].length) / 2));
      out.push(new Paragraph({ numbering: { reference: "vinetas", level: nivel }, spacing: { after: 40 },
        children: inline(l.replace(/^\s*[-*]\s+/, "")) }));
      i++;
      continue;
    }
    if (/^\s*\d+\.\s+/.test(l)) {
      out.push(new Paragraph({ numbering: { reference: "numeros", level: 0 }, spacing: { after: 40 },
        children: inline(l.replace(/^\s*\d+\.\s+/, "")) }));
      i++;
      continue;
    }
    if (l.startsWith(">")) {
      out.push(new Paragraph({ indent: { left: 400 }, border: { left: { style: BorderStyle.SINGLE, size: 12, color: "A6A6A6", space: 8 } },
        children: inline(l.replace(/^>\s?/, ""), { italics: true, color: "404040" }) }));
      i++;
      continue;
    }
    if (/^---+\s*$/.test(l) || l.trim() === "" || l.startsWith("<!--")) { i++; continue; }
    // parrafo: une lineas consecutivas
    const buf = [l];
    i++;
    while (i < lineas.length && lineas[i].trim() !== "" && !/^(#|```|\s*\||\s*[-*]\s|\s*\d+\.\s|>)/.test(lineas[i])) buf.push(lineas[i++]);
    out.push(new Paragraph({ spacing: { after: 120 }, children: inline(buf.join(" ")) }));
  }
  return out;
}

const portada = [
  new Paragraph({ spacing: { before: 2600, after: 200 }, alignment: AlignmentType.CENTER,
    children: [new TextRun({ text: tituloPortada, bold: true, size: 48, color: AZUL, font: FUENTE })] }),
  new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 400 },
    children: [new TextRun({ text: subtitulo, size: 28, color: "404040", font: FUENTE })] }),
  new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 120 },
    children: [new TextRun({ text: "DSY1107 · Desarrollo Cloud Native I · Evaluación Parcial N° 5", size: 24, font: FUENTE })] }),
  new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 120 },
    children: [new TextRun({ text: "Kafka (2 brokers + ZooKeeper) · Kafka Streams (Java) · AWS Academy (4 EC2)", size: 22, color: "595959", font: FUENTE })] }),
  new Paragraph({ alignment: AlignmentType.CENTER,
    children: [new TextRun({ text: "Noviembre 2026", size: 22, color: "595959", font: FUENTE })] }),
  new Paragraph({ children: [new PageBreak()] }),
  new Paragraph({ heading: HeadingLevel.HEADING_1, children: [new TextRun({ text: "Contenido" })] }),
  new TableOfContents("Contenido", { hyperlink: true, headingStyleRange: "1-2" }),
  new Paragraph({ children: [new TextRun({ text: "(Si el índice aparece vacío: clic derecho sobre él → Actualizar campos.)", italics: true, size: 18, color: "7F7F7F", font: FUENTE })] }),
];

const cuerpo = [];
archivos.forEach((f, k) => cuerpo.push(...convertir(fs.readFileSync(f, "utf8"), false)));

const doc = new Document({
  features: { updateFields: true },
  styles: {
    default: { document: { run: { font: FUENTE, size: 22 } } },
    paragraphStyles: [
      { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { font: FUENTE, size: 34, bold: true, color: AZUL },
        paragraph: { spacing: { before: 240, after: 160 }, keepNext: true, outlineLevel: 0 } },
      { id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { font: FUENTE, size: 28, bold: true, color: "2E75B6" },
        paragraph: { spacing: { before: 280, after: 120 }, keepNext: true, outlineLevel: 1 } },
      { id: "Heading3", name: "Heading 3", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { font: FUENTE, size: 24, bold: true, color: "404040" },
        paragraph: { spacing: { before: 200, after: 80 }, keepNext: true, outlineLevel: 2 } },
    ],
  },
  numbering: {
    config: [
      { reference: "vinetas", levels: [
        { level: 0, format: LevelFormat.BULLET, text: "•", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 540, hanging: 270 } } } },
        { level: 1, format: LevelFormat.BULLET, text: "–", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 1000, hanging: 270 } } } },
      ] },
      { reference: "numeros", levels: [
        { level: 0, format: LevelFormat.DECIMAL, text: "%1.", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 540, hanging: 300 } } } },
      ] },
    ],
  },
  sections: [{
    properties: { page: { size: { width: 11906, height: 16838 }, margin: { top: 1134, bottom: 1134, left: 1134, right: 1134 } } },
    footers: { default: new Footer({ children: [new Paragraph({ alignment: AlignmentType.RIGHT,
      children: [new TextRun({ text: tituloPortada + " · pág. ", size: 16, color: "7F7F7F", font: FUENTE }),
                 new TextRun({ children: [PageNumber.CURRENT], size: 16, color: "7F7F7F", font: FUENTE })] })] }) },
    children: [...portada, ...cuerpo],
  }],
});

Packer.toBuffer(doc).then((b) => { fs.writeFileSync(salida, b); console.log("ok", salida, b.length, "bytes"); });
