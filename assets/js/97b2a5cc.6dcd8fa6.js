"use strict";(self.webpackChunksite=self.webpackChunksite||[]).push([[880],{3905:(e,t,a)=>{a.d(t,{Zo:()=>p,kt:()=>g});var n=a(7294);function r(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function o(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function l(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?o(Object(a),!0).forEach((function(t){r(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):o(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function i(e,t){if(null==e)return{};var a,n,r=function(e,t){if(null==e)return{};var a,n,r={},o=Object.keys(e);for(n=0;n<o.length;n++)a=o[n],t.indexOf(a)>=0||(r[a]=e[a]);return r}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)a=o[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(r[a]=e[a])}return r}var s=n.createContext({}),c=function(e){var t=n.useContext(s),a=t;return e&&(a="function"==typeof e?e(t):l(l({},t),e)),a},p=function(e){var t=c(e.components);return n.createElement(s.Provider,{value:t},e.children)},u="mdxType",d={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var a=e.components,r=e.mdxType,o=e.originalType,s=e.parentName,p=i(e,["components","mdxType","originalType","parentName"]),u=c(a),m=r,g=u["".concat(s,".").concat(m)]||u[m]||d[m]||o;return a?n.createElement(g,l(l({ref:t},p),{},{components:a})):n.createElement(g,l({ref:t},p))}));function g(e,t){var a=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var o=a.length,l=new Array(o);l[0]=m;var i={};for(var s in t)hasOwnProperty.call(t,s)&&(i[s]=t[s]);i.originalType=e,i[u]="string"==typeof e?e:r,l[1]=i;for(var c=2;c<o;c++)l[c]=a[c];return n.createElement.apply(null,l)}return n.createElement.apply(null,a)}m.displayName="MDXCreateElement"},2807:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>s,contentTitle:()=>l,default:()=>d,frontMatter:()=>o,metadata:()=>i,toc:()=>c});var n=a(7462),r=(a(7294),a(3905));const o={sidebar_position:3},l="Unity Catalog",i={unversionedId:"unity-catalog",id:"unity-catalog",title:"Unity Catalog",description:"This document walks through the steps to register a Onetable synced Delta table in Unity Catalog on Databricks.",source:"@site/docs/unity-catalog.md",sourceDirName:".",slug:"/unity-catalog",permalink:"/docs/unity-catalog",draft:!1,tags:[],version:"current",sidebarPosition:3,frontMatter:{sidebar_position:3},sidebar:"docs",previous:{title:"Glue Data Catalog",permalink:"/docs/glue-catalog"},next:{title:"BigLake Metastore",permalink:"/docs/biglake-metastore"}},s={},c=[{value:"Pre-requisites",id:"pre-requisites",level:2},{value:"Steps",id:"steps",level:2},{value:"Running sync",id:"running-sync",level:3},{value:"Register the target table in Unity Catalog",id:"register-the-target-table-in-unity-catalog",level:3},{value:"Validating the results",id:"validating-the-results",level:3},{value:"Conclusion",id:"conclusion",level:2}],p={toc:c},u="wrapper";function d(e){let{components:t,...a}=e;return(0,r.kt)(u,(0,n.Z)({},p,a,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"unity-catalog"},"Unity Catalog"),(0,r.kt)("p",null,"This document walks through the steps to register a Onetable synced Delta table in Unity Catalog on Databricks."),(0,r.kt)("h2",{id:"pre-requisites"},"Pre-requisites"),(0,r.kt)("ol",null,(0,r.kt)("li",{parentName:"ol"},"Source table(s) (Hudi/Iceberg) already written to external storage locations like S3/GCS.\nIf you don't have a source table written in S3/GCS,\nyou can follow the steps in ",(0,r.kt)("a",{parentName:"li",href:"https://link-to-how-to/create/dataset.md"},"this")," tutorial to set it up."),(0,r.kt)("li",{parentName:"ol"},"Setup connection to external storage locations from Databricks.",(0,r.kt)("ul",{parentName:"li"},(0,r.kt)("li",{parentName:"ul"},"Follow the steps outlined ",(0,r.kt)("a",{parentName:"li",href:"https://docs.databricks.com/en/storage/amazon-s3.html"},"here")," for Amazon S3"),(0,r.kt)("li",{parentName:"ul"},"Follow the steps outlined ",(0,r.kt)("a",{parentName:"li",href:"https://docs.databricks.com/en/storage/gcs.html"},"here")," for Google Cloud Storage"))),(0,r.kt)("li",{parentName:"ol"},"Create a Unity Catalog metastore in Databricks as outlined ",(0,r.kt)("a",{parentName:"li",href:"https://docs.gcp.databricks.com/data-governance/unity-catalog/create-metastore.html#create-a-unity-catalog-metastore"},"here"),"."),(0,r.kt)("li",{parentName:"ol"},"Create an external location in Databricks as outlined ",(0,r.kt)("a",{parentName:"li",href:"https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-location.html"},"here"),"."),(0,r.kt)("li",{parentName:"ol"},"Clone the Onetable ",(0,r.kt)("a",{parentName:"li",href:"https://github.com/onetable-io/onetable"},"repository")," and create the\n",(0,r.kt)("inlineCode",{parentName:"li"},"utilities-0.1.0-SNAPSHOT-bundled.jar")," by following the steps on the ",(0,r.kt)("a",{parentName:"li",href:"https://link/to/installation/page"},"Installation page"))),(0,r.kt)("h2",{id:"steps"},"Steps"),(0,r.kt)("h3",{id:"running-sync"},"Running sync"),(0,r.kt)("p",null,"Create ",(0,r.kt)("inlineCode",{parentName:"p"},"my_config.yaml")," in the cloned Onetable directory."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-yaml",metastring:'md title="yaml"',md:!0,title:'"yaml"'},"sourceFormat: HUDI|ICEBERG # choose only one\ntargetFormats:\n  - DELTA\ndatasets:\n  -\n    tableBasePath: s3://path/to/source/data\n    tableName: table_name\n    partitionSpec: partitionpath:VALUE\n")),(0,r.kt)("admonition",{title:"Note:",type:"tip"},(0,r.kt)("p",{parentName:"admonition"},"Replace ",(0,r.kt)("inlineCode",{parentName:"p"},"s3://path/to/source/data")," to ",(0,r.kt)("inlineCode",{parentName:"p"},"gs://path/to/source/data")," if you have your source table in GCS.\nAnd replace with appropriate values for ",(0,r.kt)("inlineCode",{parentName:"p"},"sourceFormat"),", and ",(0,r.kt)("inlineCode",{parentName:"p"},"tableName")," fields. ")),(0,r.kt)("p",null,"From your terminal under the cloned Onetable directory, run the sync process using the below command."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-shell",metastring:'md title="shell"',md:!0,title:'"shell"'},"java -jar utilities/target/utilities-0.1.0-SNAPSHOT-bundled.jar -datasetConfig my_config.yaml\n")),(0,r.kt)("admonition",{title:"Note: ",type:"tip"},(0,r.kt)("p",{parentName:"admonition"},"At this point, if you check your bucket path, you will be able to see ",(0,r.kt)("inlineCode",{parentName:"p"},"_delta_log")," directory with\n00000000000000000000.json which contains the logs that helps query engines to interpret the source table as a Delta table.")),(0,r.kt)("h3",{id:"register-the-target-table-in-unity-catalog"},"Register the target table in Unity Catalog"),(0,r.kt)("p",null,"In your Databricks workspace, under SQL editor, run the following queries."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-sql",metastring:'md title="SQL"',md:!0,title:'"SQL"'},"CREATE CATALOG onetable;\n\nCREATE SCHEMA onetable.synced_delta_schema;\n\nCREATE TABLE onetable.synced_delta_schema.<table_name>\nUSING DELTA\nLOCATION 's3://path/to/source/data';\n")),(0,r.kt)("admonition",{title:"Note:",type:"tip"},(0,r.kt)("p",{parentName:"admonition"},"Replace ",(0,r.kt)("inlineCode",{parentName:"p"},"s3://path/to/source/data")," to ",(0,r.kt)("inlineCode",{parentName:"p"},"gs://path/to/source/data")," if you have your source table in GCS.")),(0,r.kt)("h3",{id:"validating-the-results"},"Validating the results"),(0,r.kt)("p",null,"You can now see the created delta table in ",(0,r.kt)("strong",{parentName:"p"},"Unity Catalog")," under ",(0,r.kt)("strong",{parentName:"p"},"Catalog")," as ",(0,r.kt)("inlineCode",{parentName:"p"},"<table_name>")," under\n",(0,r.kt)("inlineCode",{parentName:"p"},"synced_delta_schema")," and also query the table in the SQL editor:"),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-sql"},"SELECT * FROM onetable.synced_delta_schema.<table_name>;\n")),(0,r.kt)("h2",{id:"conclusion"},"Conclusion"),(0,r.kt)("p",null,"In this guide we saw how to,"),(0,r.kt)("ol",null,(0,r.kt)("li",{parentName:"ol"},"sync a source table to create metadata for the desired target table formats using Onetable"),(0,r.kt)("li",{parentName:"ol"},"catalog the data in Delta format in Unity Catalog on Databricks"),(0,r.kt)("li",{parentName:"ol"},"query the Delta table using Databricks SQL editor")))}d.isMDXComponent=!0}}]);