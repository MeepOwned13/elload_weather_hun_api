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
    omszTitle: {
        hun: "OMSZ állomások térképe",
        eng: "Map of OMSZ stations"
    },
    omszDesc: {
        hun: "Az alábbi oldalon láthatók az OMSZ állomásainak elhelyezkedése. A térkép alatti gombokkal megtekinthetők a korábbi mérések bármely elérhető időpontra.",
        eng: "This page shows the location of OMSZ's weather stations. The buttons below the map allow You to view the measurements of each station at any historical time."
    },
    temp: {
        hun: "Hőmérséklet",
        eng: "Temperature"
    },
    avgTemp: {
        hun: "Átlag Hőmérséklet",
        eng: "Average temperature"
    },
    prec: {
        hun: "Csapadék",
        eng: "Precipitation"
    },
    rHum: {
        hun: "Relatív páratartalom",
        eng: "Relative humidity"
    },
    gRad: {
        hun: "Globálsugárzás",
        eng: "Global radiation"
    },
    avgWS: {
        hun: "Szélsebesség",
        eng: "Wind speed"
    },

}

function langHTMLText(textOf) {
    // Used in HTML tags
    document.write(langTexts[textOf][lang])
}

function langStringText(textOf) {
    // Used in code
    return langTexts[textOf][lang]
}

