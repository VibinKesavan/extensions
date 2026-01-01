process.env.CI_TEST = "true";
global.config = () => require("../src/config").default;
