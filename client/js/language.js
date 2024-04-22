// This file should be imported at the top of the HTML
// Sets language items, allows for language choosing via button#language
// Specifies functions to allow HTML and String displays of language managed text

// currently only English and Hungarian are implemented, choosing English as baseif Hungarian is not specified in browser
let lang = localStorage.getItem("lang")
if (lang === null || lang === undefined) {
    lang = navigator.language === "hu" ? "hun" : "eng"
    localStorage.setItem("lang", lang)
}
let languageButton = null

window.onload = function() {
    languageButton = document.getElementById("language")
    let img = languageButton.getElementsByTagName("img")[0]
    img.src = lang === "hun" ? "pics/hungarian.png" : "pics/english.png"
    languageButton.addEventListener("click", () => {
        localStorage.setItem("lang", lang === "hun" ? "eng" : "hun")
        location.reload()
    })
}

const langTexts = {
    omszPage: {
        hun: "Időjárás",
        eng: "Weather",
    },
    mavirPage: {
        hun: "Áramfogyasztás",
        eng: "Electricity",
    },
    omszTitle: {
        hun: "OMSZ állomások térképe",
        eng: "Map of OMSZ stations",
    },
    omszDesc: {
        hun: "Az alábbi oldalon láthatók az OMSZ állomásainak elhelyezkedése. A térkép alatti gombokkal megtekinthetők a korábbi mérések bármely elérhető időpontra.",
        eng: "This page shows the location of OMSZ's weather stations. The buttons below the map allow You to view the measurements of each station at any historical time.",
    },
    omszMessage: { // link added after
        hun: "Az időjárási adatok forrása az ",
        eng: "The source of the weather data is ",
    },
    temp: {
        hun: "Hőmérséklet",
        eng: "Temperature",
    },
    avgTemp: {
        hun: "Átlag Hőmérséklet",
        eng: "Average temperature",
    },
    prec: {
        hun: "Csapadék",
        eng: "Precipitation",
    },
    rHum: {
        hun: "Relatív páratartalom",
        eng: "Relative humidity",
    },
    gRad: {
        hun: "Globálsugárzás",
        eng: "Global radiation",
    },
    avgWS: {
        hun: "Szélsebesség",
        eng: "Wind speed",
    },
    mavirTitle: {
        hun: "MAVIR rendszerterhelési adatok",
        eng: "MAVIR electricity load data",
    },
    mavirDesc: {
        hun: "Az alábbi grafikon mutatja a MAVIR által mért elektromos áramfogyasztási terv és tény adatokat. A grafikon alatti gombokkal megtekinthetők a mérések bármely korábbi időpontra.",
        eng: "The graph below shows the true electricity load measured and the planned electricity load by MAVIR. The buttons below the graph allow You to view the measurements at any historical time.",
    },
    mavirMessage: { // link added after
        hun: "Az elektromos áramfogyasztási adatok forrása a ",
        eng: "The source of the electricity load data is ",
    },
    showLegend: {
        hun: "Jelmagyarázat",
        eng: "Legend",
    },
    NetSystemLoad: {
        hun: "Nettó rendszerterhelés",
        eng: "Net system load",
    },
    NetSystemLoadFactPlantManagment: {
        hun: "Nettó tény rendszerterhelés - üzemirányítási",
        eng: "Net system load fact - plant managment",
    },
    NetSystemLoadNetTradeSettlement: {
        hun: "Nettó tény rendszerterhelés - net.ker.elsz.meres",
        eng: "Net system load - net trade settlement",
    },
    NetPlanSystemLoad: {
        hun: "Nettó terv rendszerterhelés",
        eng: "Net planned system load",
    },
    NetSystemLoadDayAheadEstimate: {
        hun: "Nettó rendszerterhelés becslés (dayahead)",
        eng: "Net system load estimate (dayahead)",
    },
    NetPlanSystemProduction: {
        hun: "Nettó terv rendszertermelés",
        eng: "Net planned system production",
    },
    GrossSystemLoad: {
        hun: "Bruttó rendszerterhelés",
        eng: "Gross system load",
    },
    GrossCertifiedSystemLoad: {
        hun: "Bruttó hitelesített rendszerterhelés",
        eng: "Gross certified system load",
    },
    GrossPlanSystemLoad: {
        hun: "Bruttó terv rendszerterhelés",
        eng: "Gross planned system load",
    },
    GrossSystemLoadDayAheadEstimate: {
        hun: "Bruttó rendszerterhelés becslés (dayahead)",
        eng: "Gross system load estimate (dayahead)",
    },
    aiTitle: {
        hun: "AI rendszerterhelési előrejelzések",
        eng: "AI electricity load predictions",
    },
    aiDesc: {
        hun: "Az alábbi grafikonon láthatók egy Sequence-to-Sequence Mesterséges intelligencia modell előrejelzései a nettó fogyasztásra. Az előrejelzés 3 órára (óránként) történik, az egyes vonalak az egyes távok előrejelzéseit mutatják. A grafikon alatti gombokkal megtekinthetők az előrejelzések bármely korábbi időpontra.",
        eng: "The graph below displays the predictions of a Sequence-to-Sequence Artifical intelligence model for net system load. Predictions are made for the next 3 hours (by-hour), each line displays different horizons. The buttons below the graph allow You to view the predictions at any historical time.",
    },
    NSLP1ago: {
        hun: "1 órával ezelőtti predikció",
        eng: "Prediction from 1 hour ago",
    },
    NSLP2ago: {
        hun: "2 órával ezelőtti predikció",
        eng: "Prediction from 2 hour ago",
    },
    NSLP3ago: {
        hun: "3 órával ezelőtti predikció",
        eng: "Prediction from 3 hour ago",
    },
}

/**
* Displays language managed text on HTML page
* @param {String} textOf - key for text in langTexts
*/
function langHTMLText(textOf) {
    // Used in HTML tags
    document.write(langTexts[textOf][lang])
}

/**
* Returns language managed text as String
* @param {String} textOf - key for text in langTexts
* @returns {String} language managed String
*/
function langStringText(textOf) {
    // Used in code
    if (langTexts[textOf] === undefined) return "UNDEFINED"
    return langTexts[textOf][lang]
}

