package io.github.aneudeveloper.func.engine;

import io.github.aneudeveloper.func.engine.function.FuncEvent;
import io.github.aneudeveloper.func.engine.function.FuncEventDeserializer;

public class Test {
    public static void main(String... a){
        FuncEventDeserializer funcEventDeserializer = new FuncEventDeserializer(null);

        FuncEvent deserialize = funcEventDeserializer.deserialize("""
            v=1,id=56176174-9ee7-4d4c-b8a1-d24bcdebb2e0,timestamp=1695910687818,processName=AdobeSign,func_type=WORKFLOW,comingFromId=c72320a3-6e29-4c1b-b8d8-ea081ef1dc0f,processInstanceID=757c8da4-d55d-4b28-ad36-fef0b84e648e,func=createAllTransientAdobeSignDocuments,retryCount=1,nextRetryAt=1695910987818,sourceTopic=AdobeSign-WORKFLOW,$e%,{"universalSignRequest":{"documents":[{"barcode":"AD9X0011111","type":"OVB_RO_ANALYSEPROTOKOLL","name":null,"subjectTitle":null}],"participants":[{"type":"CLIENT","firstName":"Anda_Test","lastName":"Bodea_Test","email":"Andreas.Neu_ext@ovb.email","phone":null},{"type":"AGENT","firstName":"Lucian Marius","lastName":"Bunea","email":"Andreas.Neu_ext@ovb.email","phone":null}],"country":"RO","workflow":"GDPR","client":16023628,"language":null},"agreementConfig":{"documents":[{"label":"OVB_RO_ANALYSEPROTOKOLL","barcode":"AD9X0011111","type":"OVB_RO_ANALYSEPROTOKOLL","transientDocumentId":null}],"name":"FORMULAR DE ANALIZ? A CERIN?ELOR ?I NECESIT??ILOR CLIEN?ILOR Anda_Test Bodea_Test","participants":[{"firstName":"Anda_Test","lastName":"Bodea_Test","email":"Andreas.Neu_ext@ovb.email","order":"1"}]},"agreementId":null,"docuvitaId":null,"agreementIdsForInvalidation":null,"createdYousignParticipants":null,"eSignVorgangId":null,"country":"ro","barcodeMapping":null,"eSignVorgangXApiUser":null}
        """.trim());

        System.out.println(deserialize.getRetryCount());
    }
}
