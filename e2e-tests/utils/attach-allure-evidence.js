const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PROJECT_ROOT = path.resolve(__dirname, '..');
const OUTPUT_DIR = path.join(PROJECT_ROOT, 'output');
const ALLURE_RESULTS_DIR = path.join(PROJECT_ROOT, 'allure-results');

function clearString(str) {
  if (!str) return '';
  if (str.endsWith('.')) {
    str = str.slice(0, -1);
  }
  return str
    .replace(/ /g, '_')
    .replace(/"/g, "'")
    .replace(/\//g, '_')
    .replace(/</g, '(')
    .replace(/>/g, ')')
    .replace(/:/g, '_')
    .replace(/\\/g, '_')
    .replace(/\|/g, '_')
    .replace(/\?/g, '.')
    .replace(/\*/g, '^')
    .replace(/'/g, '');
}

function listFiles(dir) {
  const result = [];
  if (!fs.existsSync(dir)) return result;
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      result.push(...listFiles(fullPath));
    } else {
      result.push(fullPath);
    }
  }
  return result;
}

function collectEvidence() {
  if (!fs.existsSync(OUTPUT_DIR)) return [];

  return listFiles(OUTPUT_DIR)
    .filter((file) => /\.(png|webm|zip)$/i.test(file))
    .map((file) => ({
      path: file,
      isTrace: /[\\/]trace[\\/].*\.zip$/i.test(file),
      isVideo: /[\\/]videos[\\/].*\.webm$/i.test(file),
      isScreenshot: /\.png$/i.test(file),
    }));
}

function findEvidenceForTest(evidence, sanitizedTitle) {
  const screenshot = evidence.find(
    (e) =>
      e.isScreenshot &&
      path.basename(e.path).startsWith(`${sanitizedTitle}_`)
  );

  const suffix = `${sanitizedTitle}.failed`;
  const video = evidence.find(
    (e) => e.isVideo && path.basename(e.path).includes(`${suffix}.webm`)
  );
  const trace = evidence.find(
    (e) => e.isTrace && path.basename(e.path).includes(`${suffix}.zip`)
  );

  return { screenshot, video, trace };
}

function contentTypeFor(evidence) {
  if (evidence.isScreenshot) return 'image/png';
  if (evidence.isVideo) return 'video/webm';
  if (evidence.isTrace) return 'application/vnd.allure.playwright-trace';
  return 'application/octet-stream';
}

function attachmentFor(evidence, label) {
  const ext = path.extname(evidence.path);
  const digest = crypto
    .createHash('md5')
    .update(path.basename(evidence.path))
    .digest('hex')
    .slice(0, 16);
  const fileName = `${digest}${ext}`;
  const target = path.join(ALLURE_RESULTS_DIR, fileName);
  if (!fs.existsSync(target)) {
    fs.copyFileSync(evidence.path, target);
  }
  return {
    name: label,
    source: fileName,
    type: contentTypeFor(evidence),
  };
}

function existsAttachment(result, source) {
  return (
    result.attachments &&
    result.attachments.some((att) => att && att.source === source)
  );
}

function attachEvidence() {
  if (!fs.existsSync(ALLURE_RESULTS_DIR)) {
    console.log('[attach-allure-evidence] Sem allure-results para processar.');
    return 0;
  }

  const evidence = collectEvidence();
  const results = fs
    .readdirSync(ALLURE_RESULTS_DIR)
    .filter((name) => name.endsWith('-result.json'))
    .map((name) => path.join(ALLURE_RESULTS_DIR, name));

  let total = 0;

  for (const resultFile of results) {
    const result = JSON.parse(fs.readFileSync(resultFile, 'utf8'));
    const title = result.name || '';

    const sanitizedTitle = clearString(title).slice(0, 100);
    if (!sanitizedTitle) continue;

    const matched = findEvidenceForTest(evidence, sanitizedTitle);
    const attachments = [];

    if (matched.screenshot) {
      attachments.push(attachmentFor(matched.screenshot, 'Screenshot (evidência)'));
    }
    if (matched.video) {
      attachments.push(attachmentFor(matched.video, 'Vídeo (evidência)'));
    }
    if (matched.trace) {
      attachments.push(attachmentFor(matched.trace, 'Trace (evidência)'));
    }

    if (!attachments.length) continue;

    if (!result.attachments) result.attachments = [];
    const before = result.attachments.length;

    for (const att of attachments) {
      if (!existsAttachment(result, att.source)) {
        result.attachments.push(att);
      }
    }

    if (result.attachments.length > before) {
      fs.writeFileSync(resultFile, JSON.stringify(result));
      total += result.attachments.length - before;
      console.log(`[attach-allure-evidence] Anexado para "${title}": ${result.attachments.length - before} evidência(s).`);
    }
  }

  console.log(`[attach-allure-evidence] Total de evidências anexadas: ${total}.`);
  return total;
}

function main() {
  return attachEvidence();
}

if (require.main === module) {
  main();
}

module.exports = { attachEvidence };
