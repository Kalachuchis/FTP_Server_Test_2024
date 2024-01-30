async function main() {
    var requestOptions = {
        method: "POST",
        redirect: "follow",
    };

    let response = await fetch(
        "http://127.0.0.1/core/loginguest?userid=bpictpw&password=Pointwest!2345678",
        requestOptions
    );
    console.log(response.headers.get("set-cookie"));
}

main();
