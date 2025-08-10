import React, { useState, useMemo } from "react";
import { ChevronUpIcon, ChevronDownIcon, XIcon } from "lucide-react";

const convertDateFormat = (dateString) => {
  const [date, time] = dateString.split(", ");
  const [day, month, year] = date.split("/");
  return `${year}-${month}-${day}T${time}`;
};

const CustomTable = ({ columns, data, itemsPerPage = 10 }) => {
  const [sortColumn, setSortColumn] = useState(null);
  const [sortDirection, setSortDirection] = useState("asc");
  const [currentPage, setCurrentPage] = useState(1);
  const [searchTerm, setSearchTerm] = useState("");
  const [filters, setFilters] = useState({});
  const [isFilterMenuOpen, setIsFilterMenuOpen] = useState(false);
  const resetFilters = () => {
    setFilters({});
  };
  const handleFilterChange = (accessor, value) => {
    setFilters((prevFilters) => ({
      ...prevFilters,
      [accessor]: value,
    }));
  };
  const removeFilter = (accessor) => {
    setFilters((prevFilters) => {
      const updatedFilters = { ...prevFilters };
      delete updatedFilters[accessor];
      return updatedFilters;
    });
  };

  const filteredData = useMemo(() => {
    return data.filter((item) => {
      const matchesSearch =
        searchTerm === "" ||
        columns.some((column) => {
          return String(item[column.accessor])
            .toLowerCase()
            .includes(searchTerm.toLowerCase());
        });

      const matchesFilters = Object.entries(filters).every(
        ([accessor, value]) => {
          if (value === "") return true;

          if (
            ["Uploaded at", "created_at", "Created At", "Created at"].includes(
              accessor,
            )
          ) {
            const [startDate, endDate] = value.split(" - ");
            const itemDate = new Date(convertDateFormat(item[accessor]));
            return (
              itemDate >= new Date(startDate) && itemDate <= new Date(endDate)
            );
          }

          if (accessor.endsWith("type") || accessor === "Uploaded by") {
            return item[accessor] === value;
          }

          if (accessor.startsWith("Size")) {
            const [operator, sizeValue] = value.split(":");
            const itemSize = Number(item[accessor]);
            const numericSizeValue = Number(sizeValue);

            if (operator === "greater") {
              return itemSize > numericSizeValue;
            } else if (operator === "less") {
              return itemSize < numericSizeValue;
            } else if (operator === "equal") {
              return itemSize === numericSizeValue;
            }
          }

          return String(item[accessor])
            .toLowerCase()
            .includes(value.toLowerCase());
        },
      );

      return matchesSearch && matchesFilters;
    });
  }, [searchTerm, filters, data, columns]);

  const sortedData = useMemo(() => {
    if (!sortColumn) return filteredData;

    return [...filteredData].sort((a, b) => {
      if (sortColumn === "#") {
        const aIndex = filteredData.indexOf(a) + 1;
        const bIndex = filteredData.indexOf(b) + 1;
        return sortDirection === "asc" ? aIndex - bIndex : bIndex - aIndex;
      }

      if (a[sortColumn] < b[sortColumn])
        return sortDirection === "asc" ? -1 : 1;
      if (a[sortColumn] > b[sortColumn])
        return sortDirection === "asc" ? 1 : -1;
      return 0;
    });
  }, [filteredData, sortColumn, sortDirection]);

  const paginatedData = useMemo(() => {
    const startIndex = (currentPage - 1) * itemsPerPage;
    return sortedData.slice(startIndex, startIndex + itemsPerPage);
  }, [sortedData, currentPage, itemsPerPage]);

  const totalPages = Math.ceil(filteredData.length / itemsPerPage);

  const handleSort = (accessor) => {
    if (sortColumn === accessor) {
      setSortDirection(sortDirection === "asc" ? "desc" : "asc");
    } else {
      setSortColumn(accessor);
      setSortDirection("asc");
    }
  };

  const handlePageChange = (page) => {
    setCurrentPage(page);
  };

  const uniqueValues = (accessor) => {
    return [...new Set(data.map((item) => item[accessor]))];
  };

  return (
    <div className="space-y-4 p-4 bg-gray-900 text-white rounded-lg shadow-lg scrollbar-always-visible">
      <div className="flex justify-between items-center mb-4 space-x-2">
        <div className="px-4 rounded bg-gray-800 text-white hover:bg-gray-700 transition">
          <button
            onClick={() => setIsFilterMenuOpen((prev) => !prev)}
            className="px-4 py-2 rounded bg-gray-800 text-white hover:bg-gray-700 transition"
          >
            Filter
          </button>
          {isFilterMenuOpen && (
            <div className="absolute -left-4 sm:left-auto bg-gray-900 p-4 rounded-lg shadow-lg z-20 border border-gray-700 mt-2 w-full sm:w-auto overflow-y-auto">
              {columns.map(
                (column) =>
                  !column.is_Action && (
                    <div
                      key={column.accessor}
                      className="flex items-center mb-2"
                    >
                      <label className="mr-2 text-gray-4  00">
                        {column.Header}:
                      </label>
                      {column.Header === "Uploaded at" ||
                      column.Header === "Created At" ? (
                        <div className="flex space-x-2">
                          <input
                            type="date"
                            value={
                              filters[column.accessor]?.split(" - ")[0] || ""
                            }
                            onChange={(e) => {
                              const endDate =
                                filters[column.accessor]?.split(" - ")[1] || "";
                              handleFilterChange(
                                column.accessor,
                                `${e.target.value} - ${endDate}`,
                              );
                            }}
                            className="px-2 py-1 rounded bg-gray-800 text-white border border-gray-700 focus:border-blue-500 focus:outline-none transition"
                          />
                          <input
                            type="date"
                            value={
                              filters[column.accessor]?.split(" - ")[1] || ""
                            }
                            onChange={(e) => {
                              const startDate =
                                filters[column.accessor]?.split(" - ")[0] || "";
                              handleFilterChange(
                                column.accessor,
                                `${startDate} - ${e.target.value}`,
                              );
                            }}
                            className="px-2 py-1 rounded bg-gray-800 text-white border border-gray-700 focus:border-blue-500 focus:outline-none transition"
                          />
                        </div>
                      ) : column.accessor.endsWith("type") ||
                        column.Header === "Uploaded by" ||
                        column.Header === "Created By" ? (
                        <select
                          value={filters[column.accessor] || ""}
                          onChange={(e) =>
                            handleFilterChange(column.accessor, e.target.value)
                          }
                          className="px-2 py-1 rounded bg-gray-800 text-white border border-gray-700 focus:border-blue-500 focus:outline-none transition"
                        >
                          <option value="">All</option>
                          {uniqueValues(column.accessor).map((value) => (
                            <option key={value} value={value}>
                              {value}
                            </option>
                          ))}
                        </select>
                      ) : column.accessor.startsWith("Size") ? (
                        <div className="flex space-x-2">
                          <select
                            value={
                              filters[column.accessor]?.split(":")[0] || ""
                            }
                            onChange={(e) => {
                              const operator = e.target.value;
                              const sizeValue =
                                filters[column.accessor]?.split(":")[1] || "";
                              handleFilterChange(
                                column.accessor,
                                `${operator}:${sizeValue}`,
                              );
                            }}
                            className="px-2 py-1 rounded bg-gray-800 text-white border border-gray-700 focus:border-blue-500 focus:outline-none transition"
                          >
                            <option value="">Select</option>
                            <option value="greater">Greater</option>
                            <option value="less">Less</option>
                            <option value="equal">Equal</option>
                          </select>
                          <input
                            type="text"
                            placeholder="Size"
                            value={
                              filters[column.accessor]?.split(":")[1] || ""
                            }
                            onChange={(e) => {
                              const operator =
                                filters[column.accessor]?.split(":")[0] || "";
                              const sizeValue = e.target.value;
                              handleFilterChange(
                                column.accessor,
                                `${operator}:${sizeValue}`,
                              );
                            }}
                            className="px-2 py-1 rounded bg-gray-800 text-white border border-gray-700 focus:border-blue-500 focus:outline-none transition"
                          />
                        </div>
                      ) : (
                        <input
                          type="text"
                          placeholder={`Filter ${column.Header}...`}
                          value={filters[column.accessor] || ""}
                          onChange={(e) =>
                            handleFilterChange(column.accessor, e.target.value)
                          }
                          className="px-2 py-1 rounded bg-gray-800 text-white border border-gray-700 focus:border-blue-500 focus:outline-none transition"
                        />
                      )}
                    </div>
                  ),
              )}

              <div className="flex space-x-2">
                <button
                  onClick={() => setIsFilterMenuOpen(false)}
                  className="mt-2 px-4 py-2 rounded bg-green-600 text-white hover:bg-green-400 transition"
                >
                  Apply Filters
                </button>
                <button
                  onClick={resetFilters}
                  className="mt-2 px-4 py-2 rounded bg-red-600 text-white hover:bg-red-400 transition"
                >
                  Reset Filters
                </button>
              </div>
            </div>
          )}
        </div>
        {/* Search input */}
        <input
          type="text"
          placeholder="Search..."
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          className="px-4 py-2 w-full rounded bg-gray-800 text-white"
        />
      </div>
      {/* Display applied filters */}
      <div className="mb-4 flex flex-wrap">
        {Object.entries(filters).map(([accessor, value]) => (
          <div
            key={accessor}
            className="flex items-center space-x-2 bg-gray-700 px-3 py-1 rounded-full text-sm mr-2 mb-2"
          >
            <span>{`${accessor}: ${value}`}</span>
            <XIcon
              className="cursor-pointer"
              size={16}
              onClick={() => removeFilter(accessor)}
            />
          </div>
        ))}
      </div>
      <div className="overflow-hidden rounded-lg border border-gray-700">
        <div className="overflow-x-auto scrollbar-always-visible">
          <div className="overflow-y-auto max-h-[calc(100vh-300px)] scrollbar-always-visible">
            <table className="min-w-full divide-y divide-gray-700 relative">
              <thead className="bg-gray-800 sticky top-0 z-10">
                <tr>
                  <th
                    onClick={() => handleSort("#")}
                    className="px-6 py-3 text-left text-xs font-medium text-green-500 uppercase tracking-wider cursor-pointer"
                  >
                    <div className="flex items-center space-x-1">
                      <span>#</span>
                      {sortColumn === "#" &&
                        (sortDirection === "asc" ? (
                          <ChevronUpIcon size={14} />
                        ) : (
                          <ChevronDownIcon size={14} />
                        ))}
                    </div>
                  </th>
                  {columns.map((column, index) => (
                    <th
                      key={column.accessor || `column-${index}`}
                      className={`px-6 py-3 text-left text-xs font-medium text-green-500 uppercase tracking-wider cursor-pointer ${
                        column.is_Action 
                          ? 'sticky right-0 bg-gray-800 z-20 shadow-lg border-l border-gray-600' 
                          : ''
                      }`}
                      onClick={() =>
                        column.accessor && handleSort(column.accessor)
                      }
                    >
                      <div className="flex items-center space-x-1">
                        <span>{column.Header}</span>
                        {sortColumn === column.accessor &&
                          (sortDirection === "asc" ? (
                            <ChevronUpIcon size={14} />
                          ) : (
                            <ChevronDownIcon size={14} />
                          ))}
                      </div>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="bg-gray-900 divide-y divide-gray-700">
                {paginatedData.length > 0 ? (
                  paginatedData.map((item, rowIndex) => (
                    <tr
                      key={item.id || `row-${rowIndex}`}
                      className={
                        rowIndex % 2 === 0 ? "bg-gray-800" : "bg-gray-900"
                      }
                    >
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-300">
                        {rowIndex + 1 + (currentPage - 1) * itemsPerPage}
                      </td>
                      {columns.map((column, columnIndex) => (
                        <td
                          key={`${item.id || rowIndex}-${column.accessor || columnIndex}`}
                          className={`px-6 py-4 whitespace-nowrap text-sm text-gray-300 ${
                            column.is_Action 
                              ? `sticky right-0 z-20 shadow-lg border-l border-gray-600 ${
                                  rowIndex % 2 === 0 ? "bg-gray-800" : "bg-gray-900"
                                }` 
                              : 'overflow-hidden truncate'
                          }`}
                          title={
                            !column.is_Action && item[column.accessor]
                              ? String(item[column.accessor])
                              : undefined
                          }
                        >
                          {column.accessor === "created_at"
                            ? item[column.accessor]
                            : column.Cell
                              ? column.Cell({
                                  value: item[column.accessor],
                                  row: item,
                                })
                              : item[column.accessor]?.length > 20
                                ? `${item[column.accessor].substring(0, 20)}...`
                                : item[column.accessor]}
                        </td>
                      ))}
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td
                      colSpan={columns.length + 1}
                      className="px-6 py-4 text-center text-sm text-gray-300"
                    >
                      No matching records found.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className="flex justify-between items-center">
        <div className="text-sm text-gray-400">
          Showing {(currentPage - 1) * itemsPerPage + 1} to{" "}
          {Math.min(currentPage * itemsPerPage, filteredData.length)} of{" "}
          {filteredData.length} entries
        </div>
        <div className="space-x-2">
          {Array.from({ length: totalPages }, (_, i) => i + 1).map((page) => (
            <button
              key={page}
              onClick={() => handlePageChange(page)}
              className={`px-3 py-1 text-sm rounded ${
                currentPage === page
                  ? "bg-blue-400 text-white"
                  : "bg-gray-800 text-gray-300 hover:bg-gray-700"
              }`}
            >
              {page}
            </button>
          ))}
        </div>
      </div>
    </div>
  );
};

export default CustomTable;
