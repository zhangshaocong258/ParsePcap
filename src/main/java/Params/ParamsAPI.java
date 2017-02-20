package Params;


import Params.DistributeParams.ClientParams;
import Params.DistributeParams.PcapParseDisParams;
import Params.DistributeParams.ServerParams;
import Params.PcapParseParams.PcapParseParams;
import Params.PortParams.PortParams;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import java.io.File;
import java.io.IOException;


public class ParamsAPI {
	private static class ParamsAPIHolder {
		private static final ParamsAPI INSTANCE = new ParamsAPI();
	}


	/**
	 * pcap包解析参数
	 * 陈维
	 */
	private PcapParseParams pcapParseParams;

	/**
	 * 分布式参数
	 * zsc
	 */
	private PcapParseDisParams pcapParseDisParams;
	private ServerParams serverParams;
	private ClientParams clientParams;
	private PortParams portParams;


	private ParamsAPI(){}

	public static final ParamsAPI getInstance() {
		return ParamsAPIHolder.INSTANCE;
	}

	private Element getRootElement(String algoParams) {
		Element classElement = null;
		try {
			SAXBuilder saxBuilder = new SAXBuilder();
			File inputFile = new File("configs/algorithmsParams.xml");
			Document document = saxBuilder.build(inputFile);

			classElement = document.getRootElement().getChild(algoParams);
		} catch (JDOMException | IOException e) {
			e.printStackTrace();
		}

		return classElement;
	}


	public PcapParseParams getPcapParseParams() {
		if (pcapParseParams == null) {
			String pcapParseParamsPath = GlobalConfig.getInstance().getPcapParseParamPath();
			Element pcapParam = getRootElement(pcapParseParamsPath);
			pcapParseParams = new PcapParseParams(pcapParam);
		}
		return pcapParseParams;
	}

	public void setPcapParseParams(PcapParseParams pcapParseParams) {
		this.pcapParseParams = pcapParseParams;
	}

	public PcapParseDisParams getPcapParseDisParams() {
		if (pcapParseDisParams == null) {
			String pcapParseDisParamsPath = GlobalConfig.getInstance().getPcapParseDisParamPath();
			Element pcapParam = getRootElement(pcapParseDisParamsPath);
			pcapParseDisParams = new PcapParseDisParams(pcapParam);
		}
		return pcapParseDisParams;
	}

	public void setPcapParseDisParams(PcapParseDisParams pcapParseDisParams) {
		this.pcapParseDisParams = pcapParseDisParams;
	}

	public ServerParams getServerParams() {
		if (serverParams == null) {
			String serverParamsPath = GlobalConfig.getInstance().getServerParamPath();
			Element serverParam = getRootElement(serverParamsPath);
			serverParams = new ServerParams(serverParam);

		}
		return serverParams;
	}

	public void setServerParams(ServerParams serverParams) {
		this.serverParams = serverParams;
	}

	public ClientParams getClientParams() {
		if (clientParams == null) {
			String clientParamsPath = GlobalConfig.getInstance().getClientParamPath();
			Element clientParam = getRootElement(clientParamsPath);
			clientParams = new ClientParams(clientParam);
		}
		return clientParams;
	}

	public void setClientParams(ClientParams clientParams) {
		this.clientParams = clientParams;
	}

	public PortParams getPortParams() {
		if (portParams == null) {
			String portParamsPath = GlobalConfig.getInstance().getPortParamPath();
			Element portParam = getRootElement(portParamsPath);
			portParams = new PortParams(portParam);
		}
		return portParams;
	}

	public void setPortParams(PortParams portParams) {
		this.portParams = portParams;
	}

}
