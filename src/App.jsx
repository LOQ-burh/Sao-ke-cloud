import React, { useState, useEffect } from "react";
import {Table, TableHeader, TableColumn, TableBody, TableRow, TableCell, Pagination, Spinner, getKeyValue} from "@nextui-org/react";
import useSWR from "swr";
import {Input} from "@nextui-org/react";
import {Button, ButtonGroup} from "@nextui-org/button";

// const [searchTerm, setSearchTerm] = useState("");
const fetcher = (...args) => fetch(...args).then((res) => res.json());


export default function App() {
  const [page, setPage] = React.useState(1);
  const [searchTerm, setSearchTerm] = useState('');
  const [searchState, setSearchState] = useState('');
  const apiUrl = import.meta.env.VITE_APP_API_BASE_URL;
  // console.log('apiUrl', apiUrl);
  
  var dataUrl = `${apiUrl}?page=${page}`;
  // dataUrl = `https://swapi.py4e.com/api/people?page=${page}`;

  if (searchState == true && searchTerm != '') {
    dataUrl = `${apiUrl}?fontbat=${searchTerm}&page=${page}`;
    setSearchState(false);
  }

  var { data, isLoading } = useSWR(dataUrl, fetcher, {
    keepPreviousData: true,
  });

  const rowsPerPage = 30;

  var pages = React.useMemo(() => {
    return data?.count ? Math.ceil(data.count / rowsPerPage) : 0;
  }, [data?.count, rowsPerPage]);

  var loadingState = isLoading || data?.results.length === 0 ? "loading" : "idle";

  // console.log('dataresults',data.results);
  // console.log('url', window.location.href);
  var txtProcessBy = '';
    txtProcessBy = (<span style={{ fontSize: "small" }}>Xử lý bởi <a href="https://thocode.com" target="_blank">ThoCode.com</a> trên Dữ liệu của <a href="https://facebook.com/mttqvietnam" target="_blank" rel="nofollow noopener noreferrer">MTTQVN</a></span>)

  function test(){
    alert('Tính năng tìm kiếm hiện đang tắt');
  }

  const searchFunction = (event) => {
    console.log('e', event.target.value);
  }


  const handleSubmit = (event) => {
    event.preventDefault()
  }

  const handleKeyDown = (event) => {
    console.log('searchTerm', event.target.value);
    if(event.key === "Enter" && searchTerm != '')
    {
      setSearchState(true);
    }
  }

  useEffect(() => {
    document.title = import.meta.env.VITE_APP_TITLE;
  }, []); 

  return (

    <div style={{ width: "100%"}}>
      <h1>
        {import.meta.env.VITE_APP_TITLE}
        <span style={{ paddingLeft: 5, fontSize: "small", fontStyle: "italic"}}></span>
      </h1>
      {txtProcessBy}
      <div style={{ fontSize: "8pt"}}>
        <ul>
          <li><a href="/data/Thong tin ung ho qua TSK VCB 0011001932418 tu 01.09 den10.09.2024.zip#"></a>Thong tin ung ho qua TSK VCB 0011001932418 tu 01.09 den10.09.2024.pdf</li>
        </ul>
      </div>

      <div style={{ paddingBottom: 5 }} className="flex w-full flex-wrap md:flex-nowrap gap-4">
        <form action="/" onSubmit={handleSubmit}>
        <Input isClearable type="search" placeholder="Tìm kiếm"
          onClear={() => console.log("input cleared")}
          onChange={(e) => {
            // searchFunction(e);
            setSearchTerm(e.target.value);
            console.log("onchange")
          }}
          onKeyDown={handleKeyDown}
          // onValueChange={() => console.log("onvaluechange")}
          // onSubmit={() => console.log("onsubmit")}
        />

        </form>
      </div>


      <Table
        aria-label="Example table with client async pagination"
        bottomContent={
          pages > 0 ? (
            <div className="flex w-full justify-center">
              <Pagination
                isCompact
                showControls
                showShadow
                color="primary"
                page={page}
                total={pages}
                onChange={(page) => setPage(page)}
              />
            </div>
          ) : null
        }
      >
        <TableHeader>
          <TableColumn key="date">Ngày GD</TableColumn>
          <TableColumn key="code">Mã GD</TableColumn>
          <TableColumn key="amount">Số tiền</TableColumn>
          <TableColumn key="notes">Note</TableColumn>
        </TableHeader>
        <TableBody
          items={data?.results ?? []}
          loadingContent={<Spinner />}
          loadingState={loadingState}
        >
          {(item) => (
            <TableRow key={item?._id}>
              {(columnKey) => <TableCell>{getKeyValue(item, columnKey)}</TableCell>}
            </TableRow>
          )}
        </TableBody>
      </Table>

      
    </div>
  );
}