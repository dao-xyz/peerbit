import { createClient, getPort } from "@peerbit/server";
import { useEffect, useState } from "react";
import { Box, Grid, Paper, Typography } from "@mui/material";
import {
	createTheme,
	responsiveFontSizes,
	ThemeProvider,
	CssBaseline
} from "@mui/material";
import { Ed25519Keypair } from "@peerbit/crypto";
let theme = createTheme({
	palette: {
		mode: "dark"
	},
	typography: {
		fontFamily: [
			"-apple-system",
			"BlinkMacSystemFont",
			'"Segoe UI"',
			"Roboto",
			'"Helvetica Neue"',
			"Arial",
			"sans-serif",
			'"Apple Color Emoji"',
			'"Segoe UI Emoji"',
			'"Segoe UI Symbol"'
		].join(",")
	}
});
theme = responsiveFontSizes(theme);

export const App = () => {
	const [client, setClient] = useState<
		Awaited<ReturnType<typeof createClient>> | undefined
	>();
	const [id, setId] = useState<string>();
	const [tcpAddress, setTCPAddress] = useState<string>();
	const [websocketAddress, setWebsocketAddress] = useState<string>();
	const [otherAddresses, setOtherAddresses] = useState<string[]>([]);

	useEffect(() => {
		Ed25519Keypair.create().then((key) => {
			return createClient(key, {
				address:
					window.location.protocol +
					"//" +
					window.location.hostname +
					":" +
					getPort(window.location.protocol)
			}).then((c) => {
				setClient(c);
				c.peer.id
					.get()
					.then((_id) => {
						setId(_id);
					})
					.catch((e) => {
						if (window.location.hostname !== "localhost") {
							alert(e);
						} else {
							console.error(e);
						}
					});
				c.peer.addresses
					.get()
					.then((addresses) => {
						const tcpAddress = addresses.find(
							(x) =>
								x.protoNames().includes("tcp") &&
								!x.protoNames().includes("ws") &&
								!x.protoNames().includes("wss")
						);
						if (tcpAddress) {
							setTCPAddress(tcpAddress.toString());
						}
						const wsAddress = addresses.find(
							(x) =>
								x.protoNames().includes("ws") || x.protoNames().includes("wss")
						);
						if (wsAddress) {
							setWebsocketAddress(wsAddress.toString());
						}
						let others = addresses.filter(
							(x) =>
								(!tcpAddress || !x.equals(tcpAddress)) &&
								(!wsAddress || !x.equals(wsAddress))
						);
						setOtherAddresses(others.map((x) => x.toString()));
					})
					.catch((e) => {
						if (window.location.hostname !== "localhost") {
							alert(e);
						} else {
							console.error(e);
						}
					});
			});
		});
	}, []);
	return (
		<ThemeProvider theme={theme}>
			<CssBaseline />
			<Box>
				<Grid container sx={{ p: 4, height: "100vh" }}>
					<Grid item container direction="column" spacing={2} maxWidth="400px">
						<Grid item container direction="row" alignItems="center" mb={2}>
							<Grid mr={2} display="flex" justifyContent="center" item>
								<img
									width="60px"
									height="auto"
									src="./android-chrome-192x192.png"
								></img>
							</Grid>
							<Grid item>
								<Typography variant="h5">Peerbit</Typography>
							</Grid>
						</Grid>
						<Grid item>
							<Typography variant="overline">Id</Typography>
							<Typography>{id}</Typography>
						</Grid>
						<Grid item>
							<Typography variant="overline">Address</Typography>
						</Grid>
						{tcpAddress && (
							<Grid item>
								<Typography variant="caption"> TCP (non-browser)</Typography>

								<Paper elevation={10}>
									<Typography
										m={2}
										sx={{ verticalAlign: "middle" }}
										variant="caption"
									>
										{tcpAddress}
									</Typography>
								</Paper>
							</Grid>
						)}
						{websocketAddress && (
							<Grid item>
								<Typography variant="caption">
									{" "}
									Websockets (browser and non-browser){" "}
								</Typography>
								<Paper elevation={10}>
									<Typography
										m={2}
										sx={{ verticalAlign: "middle" }}
										variant="caption"
									>
										{websocketAddress}
									</Typography>
								</Paper>
							</Grid>
						)}
						{otherAddresses.length > 0 && (
							<Grid item>
								<Typography variant="caption"> Other addresses</Typography>
								{otherAddresses.map((x) => {
									return (
										<Paper elevation={10}>
											<Typography
												m={2}
												sx={{ verticalAlign: "middle" }}
												variant="caption"
											>
												{x}
											</Typography>
										</Paper>
									);
								})}
							</Grid>
						)}
					</Grid>
				</Grid>
			</Box>
		</ThemeProvider>
	);
};
