export default (
  /** @type {import('preact-cli').Config} */
  config,
  /** @type {import('preact-cli').Env} */
  env,
  /** @type {import('preact-cli').Helpers} */
  helpers
) => {
  config.output.publicPath = "/";

  // use the public path in your app as 'process.env.PUBLIC_PATH'
  config.plugins.push(
    new helpers.webpack.DefinePlugin({
      "process.env.PUBLIC_PATH": JSON.stringify(
        config.output.publicPath || "/"
      ),
    })
  );

  config.plugins.push(
    new helpers.webpack.DefinePlugin({
      "process.env.API_URL": JSON.stringify("http://money_api/"),
    })
  );
};
